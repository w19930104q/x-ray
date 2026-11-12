'use strict'

var objectAssign = require('./lib/util').objectAssign
var streamToPromise = require('./lib/promisify')
var compact = require('./lib/util').compact
var isArray = require('./lib/util').isArray
var absolutes = require('./lib/absolutes')
var streamHelper = require('./lib/stream')
var encodePage = streamHelper.encodePage
var XrayReadable = streamHelper.XrayReadable
var isUrl = require('./lib/util').isUrl
var Crawler = require('x-ray-crawler')
var resolve = require('./lib/resolve')
var root = require('./lib/util').root
var params = require('./lib/params')
var debug = require('debug')('xray')
var cheerio = require('cheerio')
var walk = require('./lib/walk')
var fs = require('fs')

var CONST = {
  CRAWLER_METHODS: ['concurrency', 'throttle', 'timeout', 'driver', 'delay', 'limit', 'abort'],
  INIT_STATE: {
    stream: false,
    concurrency: Infinity,
    paginate: false,
    limit: Infinity,
    abort: false
  },
  // bounded internal output buffer for .stream()
  HIGH_WATER_MARK: 16 * 1024
}

function Xray (options) {
  var crawler = Crawler()
  options = options || {}
  var filters = options.filters || {}

  function xray (source, scope, selector) {
    var args = params(source, scope, selector)
    selector = args.selector
    source = args.source
    scope = args.context

    var state = objectAssign({}, CONST.INIT_STATE)

    var walkHTML = WalkHTML(xray, selector, scope, filters)
    var request = Request(crawler)

    /**
     * Create a crawl engine.
     *
     * The engine pulls one page at a time. A subsequent page is only
     * requested after the current page has been extracted and fully
     * handed off to the sink, so memory never grows with the number of
     * pages. Only the callback path aggregates results via `pages`.
     *
     * @param {Function} cb completion callback (may be a noop for sinks)
     * @return {Object} engine
     */
    function createEngine (cb) {
      var pages = []
      var sink = null
      var pulling = false
      var stopped = false
      var finished = false
      var called = false

      var engine = {
        attach: function (output) { sink = output },
        start: start,
        stop: stop
      }

      return engine

      function start () {
        if (pulling || stopped || finished) return
        pulling = true
        fetch(source, function (err, $) {
          if (stopped) return
          if (err) return fail(err)
          walkHTML($, onPage)
        })
      }

      function stop (done) {
        if (stopped) {
          if (done) process.nextTick(done)
          return
        }
        stopped = true
        pulling = false
        // the in-flight request is allowed to finish; its late
        // callbacks become no-ops once `stopped` is observed
        if (sink && sink.close) sink.close(done)
        else if (done) process.nextTick(done)
      }

      function fail (err) {
        if (stopped || finished) return
        stopped = true
        pulling = false
        if (sink && sink.error) return sink.error(err)
        finish(err)
      }

      function fetch (url, done) {
        if (stopped) return

        // already-parsed cheerio instance (e.g. x(html-or-\$, ...)(\$, fn))
        if (url && url.html) {
          return done(null, url)
        }

        if (isUrl(url)) {
          debug('starting at: %s', url)
          return request(url, function (err, html) {
            if (stopped) return
            if (err) return done(err)
            done(null, load(html, url))
          })
        }

        if (scope && ~scope.indexOf('@')) {
          debug('resolving to a url: %s', scope)
          // `source` is a raw string/url here (no parsed context yet),
          // so resolve receives false exactly like the original entry path
          var resolvedUrl = resolve(false, false, scope, filters)

          // ensure that a@href is a URL
          if (!isUrl(resolvedUrl)) {
            debug('%s is not a url. Skipping!', resolvedUrl)
            return done(null, load(''))
          }

          debug('resolved "%s" to a %s', scope, resolvedUrl)
          return request(resolvedUrl, function (err, html) {
            if (stopped) return
            if (err) return done(err)
            done(null, load(html, resolvedUrl))
          })
        }

        if (url) return done(null, load(url))
        debug('%s is not a url or html. Skipping!', url)
        return done(null, load(''))
      }

      function onPage (err, obj, $) {
        if (stopped) return
        if (err) return fail(err)

        var paginate = !!state.paginate
        var limit = --state.limit

        if (paginate) {
          if (isArray(obj)) pages = pages.concat(obj)
          else pages.push(obj)

          if (limit <= 0) {
            debug('reached limit, ending')
            return deliver(obj, true, function (err) {
              if (err) return fail(err)
              finish(null, pages)
            })
          }

          var url = resolve($, false, state.paginate, filters)
          debug('paginate(%j) => %j', state.paginate, url)

          if (!isUrl(url)) {
            debug('%j is not a url, finishing up', url)
            return deliver(obj, true, function (err) {
              if (err) return fail(err)
              finish(null, pages)
            })
          }

          if (state.abort && state.abort(obj, url)) {
            debug('abort check passed, ending')
            return deliver(obj, true, function (err) {
              if (err) return fail(err)
              finish(null, pages)
            })
          }

          // debug
          debug('paginating %j', url)
          isFinite(limit) && debug('%s page(s) left to crawl', limit)

          return deliver(obj, false, function (err) {
            if (err) return fail(err)
            if (stopped) return
            pulling = true
            request(url, function (err, html) {
              if (stopped) return
              if (err) return fail(err)
              var next$ = load(html, url)
              walkHTML(next$, onPage)
            })
          })
        }

        deliver(obj, true, function (err) {
          if (err) return fail(err)
          finish(null, obj)
        })
      }

      function deliver (obj, end, done) {
        if (stopped) return
        if (!sink) return process.nextTick(done)

        if (!state.paginate) {
          return sink.end(JSON.stringify(obj, true, 2), function (err) {
            if (err) return done(err)
            done()
          })
        }

        var json = encodePage(obj)

        if (sink.first) {
          sink.first = false
          return sink.write('[\n', function (err) {
            if (err) return done(err)
            writePage(json, end, done)
          })
        }

        writePage(json, end, done)
      }

      function writePage (json, end, done) {
        if (stopped) return
        // Commas are emitted *before* non-empty pages, so pages that
        // encode to nothing (empty extraction results) never produce
        // dangling commas and the document stays valid JSON.
        if (json === '') {
          if (end) return sink.end(']', done)
          return done()
        }
        if (sink.opened) {
          if (!end) return sink.write(',\n' + json, ioDone)
          return sink.end(',\n' + json + '\n]', ioDone)
        }
        sink.opened = true
        if (!end) return sink.write(json, ioDone)
        sink.end(json + '\n]', ioDone)

        function ioDone (err) {
          if (err) return done(err)
          done()
        }
      }

      function finish (err, result) {
        if (called) return
        called = true
        finished = true
        pulling = false
        if (err) return cb(err)
        cb(null, result)
      }
    }

    function node (source2, fn) {
      if (arguments.length === 1) {
        fn = source2
      } else {
        source = source2
      }

      debug('params: %j', {
        source: source,
        scope: scope,
        selector: selector
      })

      createEngine(fn || function () {}).start()
      return node
    }

    node.abort = function (validator) {
      if (!arguments.length) return state.abort
      state.abort = validator
      return node
    }

    node.paginate = function (paginate) {
      if (!arguments.length) return state.paginate
      state.paginate = paginate
      return node
    }

    node.limit = function (limit) {
      if (!arguments.length) return state.limit
      state.limit = limit
      return node
    }

    node.stream = function () {
      var engine = createEngine(function () {})
      var rs = new XrayReadable(engine, {
        highWaterMark: CONST.HIGH_WATER_MARK
      })
      engine.attach(readableSink(rs))
      // crawling starts on the stream's first read (see XrayReadable)
      return rs
    }

    node.write = function (path) {
      if (!arguments.length) return node.stream()
      var ws = fs.createWriteStream(path)
      var engine = createEngine(function () {})
      engine.attach(writableSink(ws))
      // a failed file stream must stop the crawl instead of being
      // reported as a successful, silently truncated run
      ws.on('error', function () { engine.stop(function () {}) })
      // .write(path) starts crawling immediately; the writable's own
      // buffering applies backpressure page by page
      engine.start()
      return ws
    }

    node.then = function (resHandler, errHandler) {
      return streamToPromise(node.stream()).then(resHandler, errHandler)
    }

    return node
  }

  CONST.CRAWLER_METHODS.forEach(function (method) {
    xray[method] = function () {
      if (!arguments.length) return crawler[method]()
      crawler[method].apply(crawler, arguments)
      return this
    }
  })

  return xray
}

/**
 * Sink backed by a Node Writable (e.g. an fs write stream).
 *
 * write/end honor backpressure: the callback only fires once the chunk
 * has been drained, so the crawl cannot prefetch the next page while the
 * consumer is still catching up.
 */
function writableSink (ws) {
  return {
    first: true,
    opened: false,
    write: function (chunk, done) { writeWhenReady(ws, chunk, false, done) },
    end: function (chunk, done) { writeWhenReady(ws, chunk, true, done) },
    error: function (err) {
      if (ws.destroyed) return
      ws.destroy(err)
    },
    close: function (done) {
      if (ws.destroyed || ws.writableFinished) return process.nextTick(done)
      ws.once('close', done)
      ws.destroy()
    }
  }
}

/**
 * Sink backed by the XrayReadable. Pauses the crawl while the consumer
 * applies backpressure; memory stays bounded by the high water mark.
 */
function readableSink (rs) {
  return {
    first: true,
    opened: false,
    write: function (chunk, done) { pushWhenReady(rs, chunk, false, done) },
    end: function (chunk, done) { pushWhenReady(rs, chunk, true, done) },
    error: function (err) { rs.destroy(err) },
    close: function (done) { done() }
  }
}

function writeWhenReady (ws, chunk, end, done) {
  if (ws.destroyed || ws.errorEmitted) {
    return process.nextTick(function () { done(new Error('write stream destroyed')) })
  }

  var settled = false
  var onError = function (err) {
    if (settled) return
    settled = true
    done(err)
  }
  ws.once('error', onError)

  if (end) {
    ws.end(chunk)
    ws.once('finish', function () {
      if (settled) return
      settled = true
      ws.removeListener('error', onError)
      done()
    })
    return
  }

  var ok = ws.write(chunk)
  if (ok) {
    settled = true
    ws.removeListener('error', onError)
    return done()
  }
  ws.once('drain', function () {
    if (settled) return
    settled = true
    ws.removeListener('error', onError)
    done()
  })
}

function pushWhenReady (rs, chunk, end, done) {
  var ok = rs.push(chunk)
  if (end) {
    rs.push(null)
    return done()
  }
  if (ok) return done()
  rs.waitDrain(done)
}

function Request (crawler) {
  return function request (url, fn) {
    debug('fetching %s', url)
    crawler(url, function (err, ctx) {
      if (err) return fn(err)
      debug('got response for %s with status code: %s', url, ctx.status)
      return fn(null, ctx.body)
    })
  }
}

function load (html, url) {
  html = html || ''
  var $ = html.html ? html : cheerio.load(html, { decodeEntities: false })
  if (url) $ = absolutes(url, $)
  return $
}

function WalkHTML (xray, selector, scope, filters) {
  return function walkHTML ($, fn) {
    walk(selector, function (v, k, next) {
      if (typeof v === 'string') {
        var value = resolve($, root(scope), v, filters)
        return next(null, value)
      } else if (typeof v === 'function') {
        return v($, function (err, obj) {
          if (err) return next(err)
          return next(null, obj)
        })
      } else if (isArray(v)) {
        if (typeof v[0] === 'string') {
          return next(null, resolve($, root(scope), v, filters))
        } else if (typeof v[0] === 'object') {
          var $scope = $.find ? $.find(scope) : $(scope)
          var pending = $scope.length
          var out = []

          // Handle the empty result set (thanks @jenbennings!)
          if (!pending) return next(null, out)

          return $scope.each(function (i, el) {
            var $innerscope = $scope.eq(i)
            var node = xray(scope, v[0])
            node($innerscope, function (err, obj) {
              if (err) return next(err)
              out[i] = obj
              if (!--pending) {
                return next(null, compact(out))
              }
            })
          })
        }
      }
      return next()
    }, function (err, obj) {
      if (err) return fn(err)
      fn(null, obj, $)
    })
  }
}

module.exports = Xray
