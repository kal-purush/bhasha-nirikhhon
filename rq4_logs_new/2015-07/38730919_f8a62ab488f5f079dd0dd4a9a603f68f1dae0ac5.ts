
/// <reference path='typings/node/node.d.ts'/>
/// <reference path='typings/through2/through2.d.ts'/>
/// <reference path='typings/gulp-util/gulp-util.d.ts'/>

import * as fs from 'fs';
import * as path from 'path';
import through = require('through2');
import * as gutil from 'gulp-util';

let PluginError = gutil.PluginError;

interface Option {
    rootDir: string;
}

export = function(dest: any) {
    var stream = through.obj(function(file, encoding, next) {
        if (file.isBuffer()) {
            this.emit('error', new PluginError('gulp-prefixer', 'Buffers not supported!'));
            return next();
        }

        if (file.isStream()) {
            let json = JSON.parse(file.contents);
            let diagnosticsText = 'export default {\n';
            let length = Object.keys(json).length;
            let index = 0;

            for (var error in json) {
                diagnosticsText += '    ' +
                    error.replace(/\s+/g, '_')
                    .replace(/['"\.,]/g, '')
                    .replace(/{(\d)}/g, '$1');

                diagnosticsText += ': {\n';
                diagnosticsText += '        message: \'' + error + '\',\n';
                diagnosticsText += '        status: ' + json[error].status + ',\n';
                diagnosticsText += '        code: ' + json[error].code + '\n';
                diagnosticsText += '    }';
                if (index < length - 1) {
                    diagnosticsText += ',\n';
                }
                index++;
            }
            diagnosticsText += '\n';
            diagnosticsText += '}';

            file.contents = Buffer.concat([new Buffer(diagnosticsText)]);
        }

        this.push(file);

        next();
    });

    return stream;
}