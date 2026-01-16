'use strict';

var gulp = require('gulp');
var sass = require('gulp-sass');

gulp.task('sass', function () {
  return gulp.src('src/public/sass/index.scss')
    .pipe(sass().on('error', sass.logError))
    .pipe(gulp.dest('src/public/css/'));
});

gulp.task('sass:watch', function () {
  gulp.watch('src/public/sass/index.scss', ['sass']);
});