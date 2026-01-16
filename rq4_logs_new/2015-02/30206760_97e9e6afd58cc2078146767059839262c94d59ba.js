var gulp = require('gulp');

gulp.task('images', function() {
  return gulp.src('dev/img/**/*')
    .pipe(gulp.dest('dist/assets/img/'))
});