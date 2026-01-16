'use strict';

var defaults = require('defaults');
var through2 = require('through2');
var mkdirp = require('mkdirp');
var path = require('path');

function folder(folder, opt) {
    opt = opt || {};

    var options = defaults(opt, {
        cwd: process.cwd()
    });

    if (typeof options.mode === 'string') {
        options.mode = parseInt(options.mode, 8);
    }

    var cwd = path.resolve(options.cwd);

    function saveFile(file, enc, cb) {
        var basePath;
        if (typeof folder === 'string') {
            basePath = path.resolve(cwd, folder);
        }
        if (typeof folder === 'function') {
            basePath = path.resolve(cwd, folder(file));
        }
        var writePath = path.resolve(basePath, file.relative);
        var writeFolder = path.dirname(writePath);

        // wire up new properties
        file.stat = file.stat ? file.stat : new fs.Stats();
        file.stat.mode = (options.mode || file.stat.mode);
        file.cwd = cwd;
        file.base = basePath;
        file.path = writePath;

        // mkdirp the folder the file is going in
        mkdirp(writeFolder, function (err) {
            if (err) {
                return cb(err);
            }
            writeContents(writePath, file, cb);
        });
    }

    var stream = through2.obj(saveFile);
    stream.resume();
    return stream;
}

module.exports = folder;