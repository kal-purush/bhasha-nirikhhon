
/// <reference path="typings/node/node.d.ts"/>
/// <reference path='typings/gulp/gulp.d.ts'/>
/// <reference path='typings/gulp-generate-ts-diagnostics/gulp-generate-ts-diagnostics.d.ts'/>
/// <reference path='typings/gulp-rename/gulp-rename.d.ts'/>

import gulp = require('gulp');
import rename = require('gulp-rename');
import generateTsDiagnostics = require('gulp-generate-ts-diagnostics');
import path = require('path');
import fs = require('fs');

var diagnosticMessageProps: generateTsDiagnostics.DiagnosticMessageProperties = [
    {
        name: 'message',
        type: 'string',
        optional: false,
    },
    {
        name: 'code',
        type: 'number',
        optional: false,
    },
    {
        name: 'code',
        type: 'number',
        optional: false,
    },
];

gulp.task('diag', () => {
    gulp.src('src/harness/diagnostics.json')
        .pipe(rename('diagnostics.generated.ts'))
        .pipe(generateTsDiagnostics(diagnosticMessageProps))
        .pipe(gulp.dest('src/harness/'));
});

gulp.task('default', ['generate-diagnostics']);