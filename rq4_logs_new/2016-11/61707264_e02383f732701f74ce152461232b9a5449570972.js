var gulp = require('gulp');
var concat = require('gulp-concat');


gulp.task('bundle', function () {
  return gulp
    .src([
      'src/_head.js',
      'src/screen.js',
    ])
    .pipe(concat('index.js'))
    .pipe(gulp.dest('./lib'));
});