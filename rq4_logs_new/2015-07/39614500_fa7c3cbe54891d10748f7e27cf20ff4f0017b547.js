var Gif = require('js-gif')

module.exports = function canvasToGif (canvas, opts, cb) {
  if (opts === undefined) throw new Error('cannot make a gif without a callback')
  var callback = (cb === undefined) ? opts : cb
  var gif = new Gif()
  var ctx = canvas.getContext('2d')
  var frames = opts.frames || 10
  var delay = opts.delay || 0

  gif.setRepeat(opts.repeat || 0)
  gif.setDelay(delay)
  gif.start()

  var clock = setInterval(function () {
    if (frame === frames) {
      gif.finish()
      var binaryGif = encode64(gif.stream().getData())
      var dataUrl = 'data:image/gif;base64,'+ binaryGif

      callback(dataUrl, binaryGif)
      clearInterval(clock)
    }
    else {
      gif.addFrame(ctx);
      frame++;
    }
  }, delay)
}