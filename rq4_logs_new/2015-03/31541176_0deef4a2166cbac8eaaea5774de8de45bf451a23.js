"use strict";
var gulp   = require('gulp'),
	jshint = require('gulp-jshint'),
	filter = require('gulp-filter'),
	mocha  = require('gulp-mocha'),
	xunit = require('./index');

gulp.task('default', ['test']);

gulp.task('watch', function () {
	gulp.watch('test/*.js', ['test']);
});

gulp.task('test', ['lint'], function () {
	gulp.src('test/*.js', {read: false})
		.pipe(mocha({reporter: 'spec', ui: 'bdd'}));
});

gulp.task('lint', function () {
	return gulp.src('**/*.js')
		.pipe(filter(['*', '!node_modules/**/*']))
		.pipe(jshint({node: true}))
		.pipe(jshint.reporter('default'));
});

gulp.task('endtoend', function () {
    return gulp.src(['C:/Workspace/gulp-xunit-runner/testAssemblies/*.Tests.dll'], {read: false})
        .pipe(xunit({
            executable: "C:/Workspace/gulp-xunit-runner/packages/xunit.runners.2.0.0-rc3-build2880/tools/xunit.console.exe",
            options: {
            	xml: 'test_output.xml'
            }            
        }));
});