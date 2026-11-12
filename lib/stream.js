var Readable = require('stream').Readable
var isArray = require('./util').isArray

/**
 * Encode a single page's extraction result into JSON text.
 *
 * Pages that extract an empty result encode to an empty string, so they
 * produce no output while still allowing the overall document to stay a
 * valid JSON array.
 *
 * @param {Mixed} data
 * @return {String}
 */
function encodePage (data) {
  var string = JSON.stringify(data, true, 2)
  var json = isArray(data) ? string.slice(1, -1) : string
  json = json.replace(/^\n+|\n+$/g, '')
  return json.trim() === '' ? '' : json
}

module.exports = {
  encodePage: encodePage,

  /**
   * Streaming array helper
   *
   * @param {Stream} data (optional)
   * @return {Function}
   */
  array: function stream_array (stream) {
    if (!stream) return function () {}
    var first = true

    return function _stream_array (data, end) {
      var json = encodePage(data)
      var empty = json === ''

      if (first && empty && !end) return
      if (first) { stream.write('[\n') }
      if (!first && !empty) { stream.write(',') }

      if (end) {
        stream.end(json + ']')
      } else {
        stream.write(json)
      }

      first = false
    }
  },

  /**
   * Streaming object helper
   *
   * @param {Stream} data (optional)
   * @return {Stream}
   */
  object: function stream_object (stream) {
    if (!stream) return function () {}

    return function _stream_object (data, end) {
      var json = JSON.stringify(data, true, 2)

      if (end) {
        stream.end(json)
      } else {
        stream.write(json)
      }
    }
  },

  /**
   * Bounded, backpressured readable that drives a paginated crawl.
   *
   * The crawl is started lazily on the first read and the engine only
   * advances when the consumer is ready for more data, so the internal
   * output buffer never grows beyond the readable high water mark plus
   * the single page currently being handed off.
   *
   * @param {Object} engine
   * @param {Object} options
   * @api private
   */
  XrayReadable: XrayReadable,

  waitCb: function stream_callback (stream, fn) {
    fn(function (err) {
      if (err) stream.emit('error', err)
    })
  }
}

function XrayReadable (engine, options) {
  if (!(this instanceof XrayReadable)) return new XrayReadable(engine, options)
  Readable.call(this, options)
  this._engine = engine
  this._started = false
  this._drainCb = null
}

XrayReadable.prototype = Object.create(Readable.prototype)
XrayReadable.prototype.constructor = XrayReadable

XrayReadable.prototype._read = function () {
  if (!this._started) {
    this._started = true
    this._engine.start()
  }
  if (this._drainCb) {
    var cb = this._drainCb
    this._drainCb = null
    cb()
  }
}

XrayReadable.prototype._destroy = function (err, cb) {
  this._engine.stop(function () {
    cb(err)
  })
}

XrayReadable.prototype.waitDrain = function (cb) {
  this._drainCb = cb
}
