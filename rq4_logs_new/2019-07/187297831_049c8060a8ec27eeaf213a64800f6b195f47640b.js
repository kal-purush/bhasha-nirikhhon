'use strict';
 
var gulp = require('gulp');
var sass = require('gulp-sass');

var sassFiles = 'assets/styles/sass/**/*.scss',
    cssDest = 'assets/styles/css/';

gulp.task('default', done => {

      gulp.src(sassFiles)
        .pipe(sass().on('error', sass.logError))
        .pipe(gulp.dest(cssDest));
  done();
});