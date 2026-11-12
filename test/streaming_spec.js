/* global it, describe */

/**
 * Bounded, backpressured streaming regression tests.
 *
 * These tests run fully offline against an in-memory driver so they can
 * assert request timing, buffer bounds and terminal states deterministically.
 */

var assert = require('assert')
var fs = require('fs')
var os = require('os')
var path = require('path')
var Writable = require('stream').Writable
var cheerio = require('cheerio')
var Xray = require('..')

/**
 * Build a mock driver serving `pages` HTML pages, each containing
 * `perPage` items and a "next" link (except the last page).
 *
 * The returned driver records every requested url, the maximum number of
 * concurrently in-flight requests and the time of each request.
 */
function mockDriver (pages, options) {
  options = options || {}
  var calls = []
  var inflight = 0
  var maxInflight = 0
  var requestAt = {}

  function driver (ctx, done) {
    calls.push(ctx.url)
    requestAt[calls.length] = Date.now()
    inflight++
    maxInflight = Math.max(maxInflight, inflight)
    var index = calls.length
    var delay = typeof options.delay === 'function'
      ? options.delay(index)
      : (options.delay || 1)

    setTimeout(function () {
      inflight--
      if (options.failAt === index) {
        return done(new Error('boom at page ' + index))
      }

      var empty = options.emptyPages && options.emptyPages.indexOf(index) !== -1
      var perPage = options.perPage == null ? 2 : options.perPage
      var items = ''
      for (var k = 0; k < perPage; k++) {
        var title = options.big
          ? 'x'.repeat(200) + index + '-' + k
          : 'p' + index + '-i' + k
        items += '<li class="item"><h2>' + title + '</h2><a href="/x/' +
          index + '-' + k + '">l</a></li>'
      }
      if (empty) items = ''
      var next = index < pages
        ? '<a class="next" href="http://x.test/page/' + (index + 1) + '">n</a>'
        : ''
      done(null, '<ul>' + items + '</ul>' + next)
    }, delay)
  }

  driver.calls = calls
  driver.requestAt = requestAt
  Object.defineProperty(driver, 'maxInflight', {
    get: function () { return maxInflight }
  })
  return driver
}

function paginated (x, driver, options) {
  options = options || {}
  x.driver(driver)
  return x('http://x.test/page/1', '.item', [{
    title: 'h2',
    link: 'a@href'
  }])
    .paginate('.next@href')
    .limit(options.limit || 100)
}

function collect (stream, cb) {
  var chunks = []
  stream.on('data', function (chunk) { chunks.push(chunk.toString()) })
  stream.on('error', function (err) { cb(err, chunks.join('')) })
  stream.on('end', function () { cb(null, chunks.join('')) })
}

describe('bounded streaming', function () {
  it('does not fetch before the first read', function (done) {
    var x = Xray()
    var driver = mockDriver(3)
    var rs = paginated(x, driver).stream()
    rs.pause()

    setTimeout(function () {
      assert.equal(driver.calls.length, 0, 'no request before reading')
      collect(rs, function (err, text) {
        if (err) return done(err)
        assert.equal(driver.calls.length, 3)
        assert.equal(JSON.parse(text).length, 6)
        done()
      })
      rs.resume()
    }, 30)
  })

  it('keeps at most one request in flight', function (done) {
    var x = Xray()
    var driver = mockDriver(6, { delay: 5 })
    var rs = paginated(x, driver).stream()
    collect(rs, function (err) {
      if (err) return done(err)
      assert.equal(driver.calls.length, 6)
      assert.equal(driver.maxInflight, 1, 'requests are serialized')
      done()
    })
  })

  it('never requests page N+1 before page N is delivered', function (done) {
    var x = Xray()
    var driver = mockDriver(8, { big: true, delay: 4 })
    var rs = paginated(x, driver).stream()

    // Track which page markers have been pushed into the readable.
    var pushed = {}
    var originalPush = rs.push
    rs.push = function (chunk) {
      if (chunk !== null) {
        var matches = chunk.toString().match(/"x{200}(\d+)-/g) || []
        matches.forEach(function (marker) {
          pushed[marker.match(/(\d+)-/)[1]] = true
        })
      }
      return originalPush.apply(this, arguments)
    }

    var violations = []
    var poll = setInterval(function () {
      for (var page = 2; page <= driver.calls.length; page++) {
        if (!pushed[String(page - 1)]) {
          violations.push('page ' + page + ' requested before page ' + (page - 1) + ' delivered')
        }
      }
    }, 2)

    collect(rs, function (err) {
      clearInterval(poll)
      if (err) return done(err)
      assert.equal(driver.calls.length, 8)
      assert.equal(violations.length, 0, violations.join('; '))
      assert.equal(Object.keys(pushed).length, 8)
      done()
    })
  })

  it('pauses production while a slow consumer applies backpressure', function (done) {
    var x = Xray()
    var driver = mockDriver(6, { delay: 3 })
    var rs = paginated(x, driver).stream()

    rs.on('data', function () {
      rs.pause()
      setTimeout(function () { rs.resume() }, 20)
    })
    rs.on('error', done)
    rs.on('end', function () {
      assert.equal(driver.calls.length, 6)
      assert.equal(driver.maxInflight, 1)
      done()
    })
  })

  it('bounds the internal output buffer to the high water mark plus one page', function (done) {
    var x = Xray()
    var driver = mockDriver(10, { big: true, delay: 2 })
    var rs = paginated(x, driver).stream()
    var hwm = rs.readableHighWaterMark

    var maxBuffered = 0
    var consumer = new Writable({
      highWaterMark: 16,
      write: function (chunk, enc, cb) {
        maxBuffered = Math.max(maxBuffered, rs.readableLength)
        setTimeout(cb, 15)
      }
    })

    rs.pipe(consumer)
    rs.on('error', done)
    consumer.on('finish', function () {
      assert.equal(driver.calls.length, 10)
      // a single oversized page may overshoot, but the buffer never
      // accumulates multiple pages (a full page is ~70KB here)
      assert(maxBuffered < hwm * 8, 'buffer stayed bounded, was ' + maxBuffered)
      done()
    })
  })

  it('emits valid JSON when pages are empty', function (done) {
    var x = Xray()
    var driver = mockDriver(4, { emptyPages: [1, 3] })
    var rs = paginated(x, driver).stream()
    collect(rs, function (err, text) {
      if (err) return done(err)
      var result = JSON.parse(text)
      assert.equal(driver.calls.length, 4)
      assert.deepEqual(result.map(function (i) { return i.title }), [
        'p2-i0', 'p2-i1', 'p4-i0', 'p4-i1'
      ])
      done()
    })
  })

  it('emits an empty JSON array when every page is empty', function (done) {
    var x = Xray()
    var driver = mockDriver(3, { perPage: 0 })
    var rs = paginated(x, driver).stream()
    collect(rs, function (err, text) {
      if (err) return done(err)
      assert.deepEqual(JSON.parse(text), [])
      assert.equal(driver.calls.length, 3)
      done()
    })
  })

  it('stops paginating and closes when the consumer destroys the stream', function (done) {
    var x = Xray()
    var driver = mockDriver(12, { delay: 10 })
    var rs = paginated(x, driver).stream()
    var lateWrites = 0
    var closed = false

    rs.on('data', function () {
      if (driver.calls.length >= 2 && !rs.destroyed) rs.destroy()
    })
    var pushAfterDestroy = rs.push
    rs.push = function (chunk) {
      if (rs.destroyed && chunk !== null) lateWrites++
      return pushAfterDestroy.apply(this, arguments)
    }
    rs.on('error', function () { /* destroy() without error is silent */ })
    rs.on('close', function () { closed = true })

    setTimeout(function () {
      var requestsAtStop = driver.calls.length
      setTimeout(function () {
        assert(closed, 'stream closed')
        assert.equal(lateWrites, 0, 'no writes after destroy')
        assert.equal(driver.calls.length, requestsAtStop,
          'no further pages fetched after destroy')
        assert(requestsAtStop <= 3, 'stopped early, was ' + requestsAtStop)
        done()
      }, 200)
    }, 300)
  })

  it('surfaces file write failures as errors and stops the crawl', function (done) {
    var x = Xray()
    var driver = mockDriver(10, { delay: 5 })
    var badPath = path.join(os.tmpdir(), 'xray-no-such-dir-' + Date.now(), 'out.json')
    var ws = paginated(x, driver).write(badPath)
    var errored = false
    var finished = false

    ws.on('error', function (err) {
      errored = true
      assert(err, 'underlying error is propagated')
    })
    ws.on('finish', function () { finished = true })

    setTimeout(function () {
      var requests = driver.calls.length
      setTimeout(function () {
        assert(errored, 'write error surfaced')
        assert(!finished, 'no fake successful finish')
        assert.equal(driver.calls.length, requests, 'pagination halted')
        done()
      }, 150)
    }, 200)
  })

  it('propagates underlying request errors instead of ending cleanly', function (done) {
    var x = Xray()
    var driver = mockDriver(10, { failAt: 3, delay: 5 })
    var rs = paginated(x, driver).stream()
    var endedCleanly = false
    var surfaced = null

    rs.on('data', function () {})
    rs.on('end', function () { endedCleanly = true })
    rs.on('error', function (err) { surfaced = err })
    rs.on('close', function () {
      assert(surfaced && /boom/.test(surfaced.message), 'error delivered')
      assert(!endedCleanly, 'not reported as successful end')
      assert.equal(driver.calls.length, 3)
      done()
    })
  })

  it('still aggregates results through the callback', function (done) {
    var x = Xray()
    var driver = mockDriver(3)
    paginated(x, driver)(function (err, arr) {
      if (err) return done(err)
      assert.equal(arr.length, 6)
      assert.equal(driver.calls.length, 3)
      assert.deepEqual(arr[0], { title: 'p1-i0', link: 'http://x.test/x/1-0' })
      done()
    })
  })

  it('still aggregates results through then()', function () {
    var x = Xray()
    var driver = mockDriver(2)
    return paginated(x, driver).then(function (arr) {
      assert.equal(arr.length, 4)
      assert.equal(driver.calls.length, 2)
    })
  })

  it('honors limit without fetching beyond it', function (done) {
    var x = Xray()
    var driver = mockDriver(10)
    var rs = paginated(x, driver, { limit: 3 }).stream()
    collect(rs, function (err, text) {
      if (err) return done(err)
      assert.equal(driver.calls.length, 3)
      assert.equal(JSON.parse(text).length, 6)
      done()
    })
  })

  it('honors abort', function (done) {
    var x = Xray()
    var driver = mockDriver(10)
    x.driver(driver)
    var rs = x('http://x.test/page/1', '.item', [{ title: 'h2' }])
      .paginate('.next@href')
      .limit(10)
      .abort(function (obj, nextUrl) {
        return /page\/3/.test(nextUrl)
      })
      .stream()

    collect(rs, function (err, text) {
      if (err) return done(err)
      assert.equal(driver.calls.length, 2)
      assert.equal(JSON.parse(text).length, 4)
      done()
    })
  })

  it('preserves nested extraction and filters', function (done) {
    var html = '<ul class="tags"><li>a</li><li>b</li><li>c</li></ul>' +
      '<ul class="tags"><li>d</li><li>e</li></ul>'
    var $ = cheerio.load(html)
    var x = Xray({
      filters: {
        upper: function (value) {
          return typeof value === 'string' ? value.toUpperCase() : value
        }
      }
    })

    var rs = x($, '.tags', [['li | upper']]).stream()
    collect(rs, function (err, text) {
      if (err) return done(err)
      assert.deepEqual(JSON.parse(text), [['A', 'B', 'C'], ['D', 'E']])
      done()
    })
  })

  it('writes a valid non-paginated object stream', function (done) {
    var $ = cheerio.load('<h1>title</h1>')
    var x = Xray()
    collect(x($, { title: 'h1' }).stream(), function (err, text) {
      if (err) return done(err)
      assert.deepEqual(JSON.parse(text), { title: 'title' })
      done()
    })
  })

  it('write(path) streams a valid paginated document to disk', function (done) {
    var file = path.join(os.tmpdir(), 'xray-stream-' + Date.now() + '.json')
    var x = Xray()
    var driver = mockDriver(3)
    var ws = paginated(x, driver).write(file)
    ws.on('error', done)
    ws.on('finish', function () {
      var parsed = JSON.parse(fs.readFileSync(file, 'utf8'))
      assert.equal(parsed.length, 6)
      assert.equal(driver.calls.length, 3)
      fs.unlinkSync(file)
      done()
    })
  })
})
