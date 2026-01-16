const gulp = require('gulp');
const babel = require('gulp-babel');
 
gulp.task('default', () => {
  return gulp.src('src/**/*.php')
    .pipe(babel())
    .pipe(gulp.dest('dist'));
});