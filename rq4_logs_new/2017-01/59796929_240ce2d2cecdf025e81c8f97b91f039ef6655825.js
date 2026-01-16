var ExportOutput, PassThrough, _, debug, moment,
  bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

_ = require("underscore");

moment = require("moment");

PassThrough = require("stream").PassThrough;

debug = require("debug")("sm:archiver:outputs:export");

ExportOutput = (function() {
  function ExportOutput(stream) {
    this.stream = stream;
    this.onEnd = bind(this.onEnd, this);
    this.passThrough = new PassThrough({
      objectMode: true
    });
    this.audios = [];
    this.length = 0;
    this.max = 360;
    this.size = 0;
    this.filename = this.stream.key + "-" + (moment().valueOf()) + "." + this.stream.opts.format;
    this.passThrough.on("end", this.onEnd);
    debug("Created for " + this.stream.key);
  }

  ExportOutput.prototype.append = function(audios) {
    if (!audios.length || this.ended) {
      return this;
    }
    _.each(audios, function(audio) {
      if (this.length === this.max) {
        return;
      }
      this.audios.push(audio);
      this.length++;
      return this.size += audio.length;
    }, this);
    debug("Current length for " + this.stream.key + " is " + this.length);
    return this;
  };

  ExportOutput.prototype.pipe = function(to) {
    this.passThrough.pipe(to);
    _.each(this.audios, function(audio) {
      return this.passThrough.write(audio);
    }, this);
    return this;
  };

  ExportOutput.prototype.end = function() {
    this.passThrough.end();
    return this;
  };

  ExportOutput.prototype.onEnd = function() {
    this.ended = true;
    debug("Ended for " + this.stream.key);
    return this;
  };

  return ExportOutput;

})();

module.exports = ExportOutput;

//# sourceMappingURL=export.js.map