/// <reference path="./typings/node/node.d.ts"/>
/// <reference path="./typings/gulp/gulp.d.ts"/>
/// <reference path="./typings/gulp-istanbul/gulp-istanbul.d.ts"/>
/// <reference path="./typings/gulp-mocha/gulp-mocha.d.ts"/>
/// <reference path="./typings/gulp-tslint/gulp-tslint.d.ts"/>
/// <reference path="./typings/gulp-typescript/gulp-typescript.d.ts"/>

'use strict';

/* Import dependencies */

import gulp = require('gulp')
import istanbul = require('gulp-istanbul')
import mocha = require('gulp-mocha')
import path = require('path')
import tslint = require('gulp-tslint')
import typescript = require('gulp-typescript')

/* Constant definitions */

var tsconfig = require('./tsconfig.json')

var compilerOptions = tsconfig.compilerOptions
var filesGlob = tsconfig.filesGlob
var outDir = compilerOptions.outDir

var srcDir = path.normalize('src')
var testDir = path.normalize('test')

var srcOutDir = path.join(outDir, srcDir)
var testOutDir = path.join(outDir, testDir)

var srcTsFiles = path.join(srcDir, '**', '*.ts')
var srcJsFiles = path.join(srcOutDir, '**', '*.js')
var testTsFiles = path.join(testDir, '**', '*.ts')
var testJsFiles = path.join(testOutDir, '**', '*.js')

/* Compile targets */

// Compiles the specified typescript files, writing to the given destination
function compile(files: string, destination: string): NodeJS.ReadWriteStream {
    'use strict';
    return gulp.src(files, { base: './' })
        .pipe(typescript(compilerOptions))
        .js.pipe(gulp.dest(outDir))
}

gulp.task('compile', () => {
    return compile(srcTsFiles, srcOutDir)
})

gulp.task('compile-test', () => {
    return compile(testTsFiles, testOutDir)
})

/* Static Analysis targets */

gulp.task('lint', () => {
    return gulp.src(filesGlob)
        .pipe(tslint())
        .pipe(tslint.report('prose'))
})

/* Test targets */

// Test the specified files using mocha
function test(files): NodeJS.ReadWriteStream {
    'use strict';
    return gulp.src(testJsFiles, { read: false })
        .pipe(mocha({ reporter: 'spec' }))
}

gulp.task('test', ['compile', 'compile-test'], () => {
    return test(testJsFiles)
});

gulp.task('coverage', ['compile', 'compile-test'], (cb) => {
    return gulp.src(srcJsFiles)
        .pipe(istanbul())
        .pipe(istanbul.hookRequire())
        .on('finish', () => {
        test(testJsFiles)
            .pipe(istanbul.writeReports())
            .on('end', () => { })
    })
})

/* Watch target */

gulp.task('watch', () => {
    gulp.watch(srcTsFiles, ['default'])
})

/* Common targets */

gulp.task('all', ['lint', 'compile', 'coverage'])

gulp.task('default', ['lint', 'compile'])