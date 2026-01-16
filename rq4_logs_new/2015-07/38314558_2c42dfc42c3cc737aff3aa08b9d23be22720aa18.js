'use strict';

var gulp = require('gulp'),
    sass = require('gulp-sass'),
    sourcemaps = require('gulp-sourcemaps'),
    ghPages = require('gulp-gh-pages');

gulp.task('sass', function() {
  gulp.src('./app/styles/*.scss')
    .pipe(sourcemaps.init())
    .pipe(sass().on('error', sass.logError))
    .pipe(sourcemaps.write('./maps'))
    .pipe(gulp.dest('./app/css'));
});

gulp.task('sass:watch', function() {
  gulp.watch('./app/styles/*.scss', ['sass']);
});

gulp.task('default', function() {
    gulp.start('sass:watch');
});

gulp.task('deploy', function() {
  return gulp.src('./app/**/*')
    .pipe(ghPages());
});