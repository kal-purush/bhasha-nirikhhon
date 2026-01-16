var gutil = require('gulp-util');
var through = require('through2');
var postcss = require('postcss');
var viewtorem = require('./lib/px2rem');

module.exports = function (options) {
    return through.obj(function (file, enc, cb) {
        if (file.isNull()) {
            this.push(file);
            return cb();
        }
        if (file.isStream()) {
            this.emit('error', new gutil.PluginError(PLUGIN_NAME, 'Streaming not supported'));
            return cb();
        }

        var content = postcss(viewtorem(options)).process(file.contents.toString()).css;
        file.contents = new Buffer(content);

        this.push(file);

        cb();
    });
};

