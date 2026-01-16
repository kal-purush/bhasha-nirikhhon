/**
 * Created by Cyprien on 8/6/2015.
 */
/// <reference path="typings/tsd.d.ts" />

import gutil = require('gulp-util');
import yargs = require('yargs');
import through = require('through2');

var PLUGIN_NAME = 'gulp-angular-translate'

export function addTranslation(opt) {
    var newKey = yargs.argv.key;
    var defaultValue = '## ' + newKey;

    gutil.log("key : " + newKey);

    return through.obj(function (file, enc, cb: Function) {
        if (file.isNull()) {
            // return empty file
            return cb(null, file);
        }

        if (file.isBuffer()) {
            var map = JSON.parse((<Buffer>file.contents).toString('utf-8'));

            if (map[newKey]) {
                gutil.log('key already exists in file : ' + file.path + ', use --force to override');
            } else {
                map[newKey] = defaultValue;
                var keys = Object.keys(map);
                keys = keys.sort((a, b)=> a < b ? -1 : 1);
                file.contents = new Buffer(JSON.stringify(map, keys, 4), "utf-8");
            }
        }
        //if (file.isStream()) {
        //    file.contents = file.contents.pipe(prefixStream(prefixText));
        //}

        cb(null, file);
    });
}

export function deleteTranslation(opt) {
    var newKey = yargs.argv.key;
    var defaultValue = '## ' + newKey;

    gutil.log("key : " + newKey);

    return through.obj(function (file, enc, cb: Function) {
        if (file.isNull()) {
            // return empty file
            return cb(null, file);
        }

        if (file.isBuffer()) {
            var map = JSON.parse((<Buffer>file.contents).toString('utf-8'));

            if (map[newKey]) {
                delete map[newKey];

                var keys = Object.keys(map);
                keys = keys.sort((a, b)=> a < b ? -1 : 1);
                file.contents = new Buffer(JSON.stringify(map, keys, 4), "utf-8");
            }
        }
        //if (file.isStream()) {
        //    file.contents = file.contents.pipe(prefixStream(prefixText));
        //}

        cb(null, file);
    });
}