'use strict'

var objectAssign = require('./lib/util').objectAssign
var streamToPromise = require('./lib/promisify')
var compact = require('./lib/util').compact
var isArray = require('./lib/util').isArray
var absolutes = require('./lib/absolutes')
var streamHelper = require('./lib/stream')
var isUrl = require('./lib/util').isUrl
var Crawler = require('x-ray-crawler')
var resolve = require('./lib/resolve')
var root = require('./lib/util').root
var params = require('./lib/params')
var debug = require('debug')('x-ray')
var cheerio = require('cheerio')
var enstore = require('enstore')
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
  }
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

    // template configuration. In reusable mode every run snapshots this
    // object instead of mutating it, so getters always report the
    // configured value and setters only affect subsequent runs.
    var state = objectAssign({}, CONST.INIT_STATE)
    var store = enstore()

    var walkHTML = WalkHTML(xray, selector, scope, filters)
    var request = Request(crawler)

    // when false (the default) the node behaves exactly as it always did:
    // repeated executions share the same mutable run context. When true,
    // every execution gets its own isolated context.
    var reusable = false

    // legacy context, reused between executions in the default mode
    var legacy = {
      state: state,
      store: store,
      pages: [],
      stream: undefined,
      source: source
    }

    function createRun (runSource) {
      return {
        state: objectAssign({}, state),
        store: enstore(),
        pages: [],
        stream: undefined,
        source: runSource
      }
    }

    function node (source2, fn) {
      if (reusable) {
        var runSource, callback
        if (arguments.length === 1) {
          runSource = legacy.source
          callback = source2
        } else {
          runSource = source2
          callback = fn
        }
        return start(createRun(runSource), callback)
      }

      if (arguments.length === 1) {
        fn = source2
      } else {
        legacy.source = source2
      }

      return start(legacy, fn)
    }

    function start (ctx, fn) {
      debug('params: %j', {
        source: ctx.source,
        scope: scope,
        selector: selector
      })

      var next = createNext(ctx, fn)

      if (isUrl(ctx.source)) {
        debug('starting at: %s', ctx.source)
        request(ctx.source, function (err, html) {
          if (err) return next(err)
          var $ = load(html, ctx.source)
          walkHTML($, next)
        })
      } else if (scope && ~scope.indexOf('@')) {
        debug('resolving to a url: %s', scope)
        var url = resolve(ctx.source, false, scope, filters)

        // ensure that a@href is a URL
        if (!isUrl(url)) {
          debug('%s is not a url. Skipping!', url)
          return walkHTML(load(''), next)
        }

        debug('resolved "%s" to a %s', scope, url)
        request(url, function (err, html) {
          if (err) return next(err)
          var $ = load(html, url)
          walkHTML($, next)
        })
      } else if (ctx.source) {
        var $ = load(ctx.source)
        walkHTML($, next)
      } else {
        debug('%s is not a url or html. Skipping!', ctx.source)
        return walkHTML(load(''), next)
      }

      return node
    }

    function createNext (ctx, fn) {
      return function next (err, obj, $) {
        if (err) return fn(err)
        var paginate = ctx.state.paginate
        var limit = --ctx.state.limit

        // create the stream
        if (!ctx.stream) {
          if (paginate) ctx.stream = streamHelper.array(ctx.state.stream)
          else ctx.stream = streamHelper.object(ctx.state.stream)
        }

        if (paginate) {
          if (isArray(obj)) {
            ctx.pages = ctx.pages.concat(obj)
          } else {
            ctx.pages.push(obj)
          }

          if (limit <= 0) {
            debug('reached limit, ending')
            ctx.stream(obj, true)
            return fn(null, ctx.pages)
          }

          var url = resolve($, false, paginate, filters)
          debug('paginate(%j) => %j', paginate, url)

          if (!isUrl(url)) {
            debug('%j is not a url, finishing up', url)
            ctx.stream(obj, true)
            return fn(null, ctx.pages)
          }

          if (ctx.state.abort && ctx.state.abort(obj, url)) {
            debug('abort check passed, ending')
            ctx.stream(obj, true)
            return fn(null, ctx.pages)
          }

          ctx.stream(obj)

          // debug
          debug('paginating %j', url)
          isFinite(limit) && debug('%s page(s) left to crawl', limit)

          request(url, function (err, html) {
            if (err) return next(err)
            var $ = load(html, url)
            walkHTML($, next)
          })
        } else {
          ctx.stream(obj, true)
          fn(null, obj)
        }
      }
    }

    node.reusable = function (flag) {
      reusable = arguments.length ? Boolean(flag) : true
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
      if (reusable) {
        var run = createRun(legacy.source)
        var writable = run.store.createWriteStream()
        run.state.stream = writable
        var rs = run.store.createReadStream()
        streamHelper.waitCb(rs, function (finish) {
          start(run, finish)
        })
        return rs
      }

      state.stream = store.createWriteStream()
      var readStream = store.createReadStream()
      streamHelper.waitCb(readStream, node)
      return readStream
    }

    node.write = function (path) {
      if (!arguments.length) return node.stream()

      if (reusable) {
        var file = fs.createWriteStream(path)
        var run = createRun(legacy.source)
        run.state.stream = file
        streamHelper.waitCb(file, function (finish) {
          start(run, function (err) {
            // make sure a failed run still releases the file handle,
            // without touching any other run's stream
            if (err) {
              try { file.end() } catch (e) { debug('error ending stream: %s', e.message) }
            }
            finish(err)
          })
        })
        return file
      }

      state.stream = fs.createWriteStream(path)
      streamHelper.waitCb(state.stream, node)
      return state.stream
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
