var gulp = require('gulp');
var minifyInline = require('gulp-minify-inline');
var minifyhtml = require('gulp-minify-html');

gulp.task('minify-css-inline', function() {
  gulp.src('src/*.html')
    .pipe(minifyInline())
    .pipe(gulp.dest('dist/'))
});

gulp.task('minify-html', function () {
  var opts = {
    empty: true,
    comments: true
  };

  gulp.src('dist/*.html')
    .pipe(minifyhtml(opts))
    .pipe(gulp.dest("dist/"))
});

gulp.task('default', ['minify-css-inline', 'minify-html']);