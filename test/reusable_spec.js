/* global it, describe, beforeEach */

/**
 * Module dependencies.
 *
 * Every test below drives x-ray through an in-memory mock driver, so the
 * suite is fully deterministic and never touches the network.
 */

var assert = require('assert')
var cheerio = require('cheerio')
var fs = require('fs')
var join = require('path').join
var rm = require('rimraf').sync
var Xray = require('..')

/**
 * Build a mock crawler driver.
 *
 * Pages are generated from `routes` (a map of pathname -> body builder).
 * The returned `ctx` exposes `counts` (request counts per url), `fail()`
 * (force the next request to error) and `delay(ms)`.
 */
function mockDriver (routes) {
  var ctx = {
    counts: {},
    shouldFail: false,
    delayMs: 0
  }

  function driver (httpCtx, next) {
    var url = httpCtx.url
    ctx.counts[url] = (ctx.counts[url] || 0) + 1

    var respond = function () {
      if (ctx.shouldFail) {
        ctx.shouldFail = false
        return next(new Error('boom: ' + url))
      }

      var path = url.replace(/^https?:\/\/[^/]+/, '').split('?')[0] || '/'
      var body = routes[path] ? routes[path](url) : ''
      next(null, body)
    }

    if (ctx.delayMs) setTimeout(respond, ctx.delayMs)
    else process.nextTick(respond)
  }

  driver.ctx = ctx
  driver.fail = function () { ctx.shouldFail = true; return driver }
  driver.delay = function (ms) { ctx.delayMs = ms; return driver }
  return driver
}

function page (n, total) {
  var next = n < total
    ? '<a class="next" href="http://example.test/page-' + (n + 1) + '">next</a>'
    : ''
  return '<html><body>' +
    '<h1>page ' + n + '</h1>' +
    '<ul class="items">' +
    '<li><a href="http://example.test/item-' + n + '-a">a' + n + '</a></li>' +
    '<li><a href="http://example.test/item-' + n + '-b">b' + n + '</a></li>' +
    '</ul>' + next +
    '</body></html>'
}

function multiPageDriver (total) {
  var routes = {}
  for (var i = 1; i <= total; i++) {
    (function (n) {
      routes['/page-' + n] = function () { return page(n, total) }
    })(i)
  }
  return mockDriver(routes)
}

describe('node.reusable()', function () {
  it('is off by default and the node keeps its legacy single-run semantics', function (done) {
    var driver = multiPageDriver(2)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', { title: 'h1' })

    node(function (err, obj) {
      if (err) return done(err)
      assert.deepEqual(obj, { title: 'page 1' })

      // legacy behavior: repeated callbacks share the same stream/store
      // context (the run is not isolated)
      node(function (err, obj2) {
        if (err) return done(err)
        assert.deepEqual(obj2, { title: 'page 1' })
        assert.equal(driver.ctx.counts['http://example.test/page-1'], 2)
        done()
      })
    })
  })

  it('keeps pagination state private to each reusable run', function (done) {
    var driver = multiPageDriver(4)
    var x = Xray().driver(driver)
    var selector = function () {
      return x('http://example.test/page-1', '.items li', [{ name: 'a' }])
        .paginate('.next@href')
        .limit(2)
    }

    var node = selector().reusable()
    node(function (err, a) {
      if (err) return done(err)
      assert.equal(a.length, 4)
      assert.deepEqual(a.map(function (i) { return i.name }),
        ['a1', 'b1', 'a2', 'b2'])
      node(function (err, b) {
        if (err) return done(err)
        assert.equal(b.length, 4, 'second run starts from zero pages/limit')
        assert.deepEqual(b, a)
        assert.equal(driver.ctx.counts['http://example.test/page-1'], 2)
        assert.equal(driver.ctx.counts['http://example.test/page-2'], 2)
        done()
      })
    })
  })

  it('runs the same selector repeatedly with isolated callbacks', function (done) {
    var driver = multiPageDriver(3)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', { title: 'h1' }).reusable()

    node(function (err, obj) {
      if (err) return done(err)
      assert.deepEqual(obj, { title: 'page 1' })

      node(function (err, obj2) {
        if (err) return done(err)
        assert.deepEqual(obj2, { title: 'page 1' })

        node(function (err, obj3) {
          if (err) return done(err)
          assert.deepEqual(obj3, { title: 'page 1' })
          assert.equal(driver.ctx.counts['http://example.test/page-1'], 3,
            'each run performs its own request')
          done()
        })
      })
    })
  })

  it('node(source, cb) only overrides the source for that run', function (done) {
    var driver = multiPageDriver(3)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', { title: 'h1' }).reusable()

    node('http://example.test/page-2', function (err, obj) {
      if (err) return done(err)
      assert.deepEqual(obj, { title: 'page 2' })

      // the template source is untouched
      node(function (err, obj2) {
        if (err) return done(err)
        assert.deepEqual(obj2, { title: 'page 1' })

        node('http://example.test/page-3', function (err, obj3) {
          if (err) return done(err)
          assert.deepEqual(obj3, { title: 'page 3' })
          node(function (err, obj4) {
            if (err) return done(err)
            assert.deepEqual(obj4, { title: 'page 1' })
            assert.equal(driver.ctx.counts['http://example.test/page-1'], 2)
            assert.equal(driver.ctx.counts['http://example.test/page-2'], 1)
            assert.equal(driver.ctx.counts['http://example.test/page-3'], 1)
            done()
          })
        })
      })
    })
  })

  it('isolates pagination counters and accumulated pages between runs', function (done) {
    var driver = multiPageDriver(4)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', '.items li', [{ name: 'a' }])
      .paginate('.next@href')
      .limit(2)
      .reusable()

    node(function (err, first) {
      if (err) return done(err)
      assert.equal(first.length, 4, 'two pages x two items')
      assert.deepEqual(first.map(function (i) { return i.name }),
        ['a1', 'b1', 'a2', 'b2'])
      assert.equal(driver.ctx.counts['http://example.test/page-1'], 1)
      assert.equal(driver.ctx.counts['http://example.test/page-2'], 1)

      // run again: counters must start fresh
      node(function (err, second) {
        if (err) return done(err)
        assert.equal(second.length, 4)
        assert.deepEqual(second, first)
        assert.equal(driver.ctx.counts['http://example.test/page-1'], 2)
        assert.equal(driver.ctx.counts['http://example.test/page-2'], 2)
        done()
      })
    })
  })

  it('interleaves concurrent runs without mixing pages or results', function (done) {
    var driver = multiPageDriver(4)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', '.items li', [{ name: 'a' }])
      .paginate('.next@href')
      .limit(2)
      .reusable()

    var doneCount = 0
    var sources = [
      'http://example.test/page-1',
      'http://example.test/page-2'
    ]

    sources.forEach(function (source, index) {
      node(source, function (err, arr) {
        if (err) return done(err)
        var base = index + 1
        assert.equal(arr.length, 4)
        assert.deepEqual(arr.map(function (i) { return i.name }), [
          'a' + base, 'b' + base,
          'a' + (base + 1), 'b' + (base + 1)
        ])
        if (++doneCount === 2) {
          // every page requested exactly once per run
          assert.equal(driver.ctx.counts['http://example.test/page-1'], 1)
          assert.equal(driver.ctx.counts['http://example.test/page-2'], 2)
          assert.equal(driver.ctx.counts['http://example.test/page-3'], 1)
          done()
        }
      })
    })
  })

  it('snapshots pagination config when each run starts', function (done) {
    var driver = multiPageDriver(4)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', '.items li', [{ name: 'a' }])
      .paginate('.next@href')
      .limit(1)
      .reusable()

    // change the limit while the first run is in flight; the running
    // snapshot must not be affected, only the next run picks it up
    node(function (err, first) {
      if (err) return done(err)
      assert.equal(first.length, 2, 'running run keeps limit=1')
      assert.equal(node.limit(), 3, 'getter reports the configured value')

      node(function (err, second) {
        if (err) return done(err)
        assert.equal(second.length, 6, 'next run uses limit=3')
        assert.equal(driver.ctx.counts['http://example.test/page-1'], 2)
        done()
      })
    })
    node.limit(3)
  })

  it('getters always return the configured template values', function () {
    var driver = multiPageDriver(2)
    var x = Xray().driver(driver)
    var abort = function () { return false }
    var node = x('http://example.test/page-1', 'h1')
      .paginate('.next@href')
      .limit(2)
      .abort(abort)
      .reusable()

    assert.equal(node.paginate(), '.next@href')
    assert.equal(node.limit(), 2)
    assert.equal(node.abort(), abort)
  })

  it('keeps abort validators isolated per run and honors the snapshot', function (done) {
    var driver = multiPageDriver(4)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', '.items li', [{ name: 'a' }])
      .paginate('.next@href')
      .limit(4)
      .abort(function (obj, nextUrl) {
        return nextUrl.indexOf('page-3') !== -1
      })
      .reusable()

    node(function (err, first) {
      if (err) return done(err)
      assert.equal(first.length, 4, 'abort after page 2 => 2 pages')

      // swap the validator between runs; the second run uses the new one
      node.abort(function (obj, nextUrl) {
        return nextUrl.indexOf('page-2') !== -1
      })

      node(function (err, second) {
        if (err) return done(err)
        assert.equal(second.length, 2, 'abort after page 1 => 1 page')
        done()
      })
    })
  })

  it('keeps a failed run from poisoning other runs and allows re-runs', function (done) {
    var driver = multiPageDriver(3)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', { title: 'h1' }).reusable()

    driver.fail()
    node(function (err) {
      assert(err instanceof Error, 'the failed run yields an error')
      assert.equal(err.message, 'boom: http://example.test/page-1')

      // error state must not leak: re-running succeeds
      node(function (err, obj) {
        if (err) return done(err)
        assert.deepEqual(obj, { title: 'page 1' })

        // another run interleaved with a second failure is unaffected
        driver.fail()
        var pending = 2
        node(function (err) {
          assert(err instanceof Error)
          if (--pending === 0) done()
        })
        node(function (err, obj) {
          if (err) return done(err)
          assert.deepEqual(obj, { title: 'page 1' })
          if (--pending === 0) done()
        })
      })
    })
  })

  it('completes each run exactly once even when paginating', function (done) {
    var driver = multiPageDriver(3)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', '.items li', [{ name: 'a' }])
      .paginate('.next@href')
      .limit(3)
      .reusable()

    var calls = 0
    node(function (err, arr) {
      if (err) return done(err)
      calls++
      assert.equal(calls, 1)
      assert.equal(arr.length, 6)
      // give the event loop a turn to expose a double-callback bug
      setTimeout(function () {
        assert.equal(calls, 1, 'callback must fire exactly once')
        done()
      }, 20)
    })
  })

  it('returns independent streams with isolated JSON delimiters', function (done) {
    var driver = multiPageDriver(3)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', '.items li', [{ name: 'a' }])
      .paginate('.next@href')
      .limit(2)
      .reusable()

    var concat = require('concat-stream')
    var results = []
    var pending = 2

    function collect () {
      node.stream().pipe(concat(function (buf) {
        results.push(JSON.parse(buf.toString()))
        if (--pending === 0) {
          assert.equal(results.length, 2)
          results.forEach(function (arr) {
            assert.equal(arr.length, 4)
            assert.deepEqual(arr.map(function (i) { return i.name }),
              ['a1', 'b1', 'a2', 'b2'])
          })
          // two independent enstore streams => each emitted its own '['
          assert.equal(driver.ctx.counts['http://example.test/page-1'], 2)
          done()
        }
      }))
    }

    collect()
    collect()
  })

  it('writes each run to its own file path', function (done) {
    var driver = multiPageDriver(2)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', '.items li', [{ name: 'a' }])
      .paginate('.next@href')
      .limit(2)
      .reusable()

    var path1 = join(__dirname, 'reusable-1.json')
    var path2 = join(__dirname, 'reusable-2.json')
    rm(path1); rm(path2)

    node.write(path1).on('finish', function () {
      node.write(path2).on('finish', function () {
        var one = JSON.parse(fs.readFileSync(path1, 'utf8'))
        var two = JSON.parse(fs.readFileSync(path2, 'utf8'))
        assert.equal(one.length, 4)
        assert.equal(two.length, 4)
        assert.deepEqual(two, one)
        assert.equal(driver.ctx.counts['http://example.test/page-1'], 2)
        rm(path1); rm(path2)
        done()
      }).on('error', done)
    }).on('error', done)
  })

  it('releases the file handle when a write run fails', function (done) {
    var driver = multiPageDriver(2)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', { title: 'h1' }).reusable()
    var path = join(__dirname, 'reusable-failed.json')
    rm(path)

    driver.fail()
    node.write(path).on('error', function (err) {
      assert(err instanceof Error)
      // the stream must have ended on its own; subsequent successful run works
      node(function (err, obj) {
        if (err) return done(err)
        assert.deepEqual(obj, { title: 'page 1' })
        rm(path)
        done()
      })
    })
  })

  it('shares a promise result within a run but not across runs', function () {
    var driver = multiPageDriver(2)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', { title: 'h1' }).reusable()

    var p1 = node.then()
    var p1Again = node.then(function (v) { return v })

    // each `.then()` is its own run (standard promise semantics apply only
    // *within* the returned promise), so two requests are made
    return Promise.all([p1, p1Again]).then(function (values) {
      assert.deepEqual(values[0], { title: 'page 1' })
      assert.deepEqual(values[1], { title: 'page 1' })
      assert.equal(driver.ctx.counts['http://example.test/page-1'], 2)

      // standard promise semantics: handlers attached to the same promise
      // observe the single shared result without new requests
      var before = driver.ctx.counts['http://example.test/page-1']
      return p1.then(function (v) {
        assert.deepEqual(v, { title: 'page 1' })
        assert.equal(driver.ctx.counts['http://example.test/page-1'], before)
      })
    })
  })

  it('propagates run failures as promise rejections independently', function () {
    var driver = multiPageDriver(2)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', { title: 'h1' }).reusable()

    driver.fail()
    var failing = node.then().then(
      function () { throw new Error('should have rejected') },
      function (err) { return err }
    )
    var healthy = node.then()

    return Promise.all([failing, healthy]).then(function (results) {
      assert(results[0] instanceof Error)
      assert.deepEqual(results[1], { title: 'page 1' })
    })
  })

  it('isolates reusable nested nodes shared by different parent inputs', function (done) {
    var driver = multiPageDriver(2)
    var x = Xray().driver(driver)

    // a reusable inner node resolving links from parent HTML
    var inner = x('a@href', { heading: 'h1' }).reusable()
    var outer = x({ page: inner })

    var htmlA = cheerio.load('<div><a href="http://example.test/page-1">p1</a></div>')
    var htmlB = cheerio.load('<div><a href="http://example.test/page-2">p2</a></div>')

    var pending = 2
    var seen = {}
    function check (err, obj, expected) {
      if (err) return done(err)
      assert.deepEqual(obj, { page: { heading: expected } })
      seen[expected] = true
      if (--pending === 0) {
        assert.deepEqual(Object.keys(seen).sort(), ['page 1', 'page 2'])
        assert.equal(driver.ctx.counts['http://example.test/page-1'], 1)
        assert.equal(driver.ctx.counts['http://example.test/page-2'], 1)
        done()
      }
    }

    outer(htmlA, function (err, obj) { check(err, obj, 'page 1') })
    outer(htmlB, function (err, obj) { check(err, obj, 'page 2') })
  })

  it('accepts raw HTML sources and cheerio instances run after run', function (done) {
    var driver = multiPageDriver(2)
    var x = Xray().driver(driver)
    var node = x('h1').reusable()

    node('<h1>one</h1>', function (err, one) {
      if (err) return done(err)
      assert.equal(one, 'one')
      node(cheerio.load('<h1>two</h1>'), function (err, two) {
        if (err) return done(err)
        assert.equal(two, 'two')
        node('<h1>three</h1>', function (err, three) {
          if (err) return done(err)
          assert.equal(three, 'three')
          done()
        })
      })
    })
  })

  it('shares the Xray instance concurrency/throttle settings across runs', function (done) {
    // a driver that observes how many requests are in flight at once
    var inflight = 0
    var peak = 0
    var driver = function (httpCtx, next) {
      inflight++
      peak = Math.max(peak, inflight)
      var n = Number(httpCtx.url.match(/page-(\d+)/)[1])
      setTimeout(function () {
        inflight--
        next(null, page(n, 8))
      }, 15)
    }

    var x = Xray().concurrency(2).driver(driver)
    var node = x('http://example.test/page-1', '.items li', [{ name: 'a' }])
      .paginate('.next@href')
      .limit(4)
      .reusable()

    // the Xray-level setting is shared and still readable
    assert.equal(x.concurrency(), 2)

    var pending = 2
    node(function (err) {
      if (err) return done(err)
      if (--pending === 0) {
        assert.ok(peak <= 2, 'concurrency cap honored across interleaved runs (peak=' + peak + ')')
        assert.ok(peak >= 2, 'requests actually overlapped (peak=' + peak + ')')
        done()
      }
    })
    node('http://example.test/page-5', function (err) {
      if (err) return done(err)
      if (--pending === 0) {
        assert.ok(peak <= 2, 'concurrency cap honored across interleaved runs (peak=' + peak + ')')
        done()
      }
    })
  })

  it('can be toggled back off to restore legacy behavior', function (done) {
    var driver = multiPageDriver(2)
    var x = Xray().driver(driver)
    var node = x('http://example.test/page-1', { title: 'h1' }).reusable()

    node(function (err, obj) {
      if (err) return done(err)
      assert.deepEqual(obj, { title: 'page 1' })

      node.reusable(false)
      node(function (err, obj2) {
        if (err) return done(err)
        assert.deepEqual(obj2, { title: 'page 1' })
        node(function (err, obj3) {
          if (err) return done(err)
          assert.deepEqual(obj3, { title: 'page 1' })
          // one reusable request + two legacy requests
          assert.equal(driver.ctx.counts['http://example.test/page-1'], 3)
          done()
        })
      })
    })
  })

  describe('all four exits on one reusable node', function () {
    var driver, x, node, concat
    beforeEach(function () {
      driver = multiPageDriver(3)
      x = Xray().driver(driver)
      concat = require('concat-stream')
      node = x('http://example.test/page-1', '.items li', [{ name: 'a' }])
        .paginate('.next@href')
        .limit(2)
        .reusable()
    })

    it('exits via node(cb)', function (done) {
      node(function (err, arr) {
        if (err) return done(err)
        assert.equal(arr.length, 4)
        done()
      })
    })

    it('exits via stream()', function (done) {
      node.stream().pipe(concat(function (buf) {
        assert.equal(JSON.parse(buf.toString()).length, 4)
        done()
      }))
    })

    it('exits via write(path)', function (done) {
      var path = join(__dirname, 'reusable-exits.json')
      rm(path)
      node.write(path).on('finish', function () {
        assert.equal(JSON.parse(fs.readFileSync(path, 'utf8')).length, 4)
        rm(path)
        done()
      }).on('error', done)
    })

    it('exits via then()', function () {
      return node.then().then(function (arr) {
        assert.equal(arr.length, 4)
      })
    })
  })
})
