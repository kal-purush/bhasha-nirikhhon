'use strict';

const gulp = require('gulp');
const browserSync = require('browser-sync').create();
const handlebars = require('gulp-compile-handlebars');
const rename = require('gulp-rename');

const config = {
  src: './',
  dest: '../',
  watchers: [
    {
      match: ['./markup/**/*.hbs'],
      tasks: ['html']
    }
  ]
};

gulp.task('html', () => {
  return gulp.src('./markup/pages/*.hbs')
    .pipe(handlebars({}, {
      ignorePartials: true,
      batch: ['./markup/partials']
    }))
    .pipe(rename({
      extname: '.html'
    }))
    .pipe(gulp.dest(config.dest));
});


gulp.task('serve', () => {
  browserSync.init({
    open: false,
    notify: false,
    files: ['./**/*'],
    server: config.dest
  });
});

gulp.task('watch', () => {
  config.watchers.forEach(item => {
    gulp.watch(item.match, item.tasks);
  });
});

gulp.task('default', ['html'], done => {
    gulp.start('serve');
    gulp.start('watch');
  done();
});