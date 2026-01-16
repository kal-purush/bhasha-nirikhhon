'use strict';

var gulp = require('gulp');
//var rename = require('gulp-rename');
var inlineCss = require('gulp-inline-css');
var htmlToJs = require('gulp-html-to-js');
var gulpMerge = require('gulp-merge');
var gulpConcat = require('gulp-concat');
var gulpMinify = require('gulp-minify');

gulp.task('default', function() {
  return gulpMerge( gulp.src('./src/template.html')
      .pipe(inlineCss())
      .pipe(htmlToJs({ global: 'window.templates', concat: 'tempaltes.js'})),
      gulp.src('./src/authPrompt.js')
    )
    .pipe(gulpConcat('authPromptPlugin.js'))
    .pipe(gulpMinify({ mangle: false, compress: true }))
    .pipe(gulp.dest('build/'));
});


