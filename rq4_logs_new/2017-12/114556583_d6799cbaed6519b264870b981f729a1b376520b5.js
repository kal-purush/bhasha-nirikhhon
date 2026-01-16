var gulp = require('gulp');
var postcss = require('gulp-postcss');
var cssnext = require('postcss-cssnext');
var babel = require('gulp-babel');
var uglify = require('gulp-uglify');
var cssnano = require('cssnano');
var clean = require('gulp-clean')

gulp.task('clean', function() {
  return gulp.src('dist/*.*', { read: false })
  .pipe(clean());
});

gulp.task('css', function() {
  return gulp.src('css/*.css')
  .pipe(postcss([
    cssnext(),
    cssnano()
  ]))
  .pipe(gulp.dest('dist/'))
})

gulp.task('js', function() {
  return gulp.src('js/*.js')
  .pipe(babel())
  .pipe(uglify())
  .pipe(gulp.dest('dist/'))
})

gulp.task('default', ['clean', 'css', 'js'])