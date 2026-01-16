'use strict';
 
var gulp = require('gulp');
var sass = require('gulp-sass');
var coffee = require('gulp-coffee'); 

var define = {
  sass: 
    {
      path: './sass/**/*.scss',
      dest: './css'
    },
  coffee:
  {
    path: './coffee/**/*.coffee',
    dest: './javascript'
  }
}

// sass task
gulp.task('sass', function () {
  gulp.src(define.sass.path)
    .pipe(sass.sync().on('error', sass.logError))
    .pipe(gulp.dest(define.sass.dest));
});

// coffee task
gulp.task('coffee', function () {
  gulp.src(define.coffee.path)
    .pipe(coffee())
    .pipe(gulp.dest(define.coffee.dest));
});

gulp.task('watch', function () {
  gulp.watch(define.sass.path, ['sass']);
  gulp.watch(define.coffee.path, ['coffee']);
});

// The default task (called when you run `gulp` from cli)
gulp.task('default', ['sass', 'coffee']);