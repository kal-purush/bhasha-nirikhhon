
/// <reference path="typings/node/node.d.ts"/>
/// <reference path='typings/gulp/gulp.d.ts'/>
/// <reference path='typings/gulp-generate-ts-diagnostics/gulp-generate-ts-diagnostics.d.ts'/>
/// <reference path='typings/gulp-rename/gulp-rename.d.ts'/>

import gulp = require('gulp');
import rename = require('gulp-rename');
import path = require('path');
import fs = require('fs');

declare function require(path: string): any;
var generateTsDiagnostics = require('./built/index');

var diagnosticMessageProps = [
    {
        name: 'status',
        type: 'number',
        optional: false,
    },
    {
        name: 'code',
        type: 'number',
        optional: false,
    },
];

gulp.task('generate-diagnostics', () => {
    gulp.src('test/diagnostics.json')
        .pipe(rename('diagnostics.generated.ts'))
        .pipe(generateTsDiagnostics(diagnosticMessageProps))
        .pipe(gulp.dest('test'));
});

gulp.task('default', ['generate-diagnostics']);