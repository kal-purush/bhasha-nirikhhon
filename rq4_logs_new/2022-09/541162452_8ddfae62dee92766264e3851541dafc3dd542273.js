var gulp = require('gulp');
var sass = require('gulp-sass')(require('sass'));
var browserSync = require('browser-sync').create();

gulp.task('compileSass', () => {
  return gulp.src('src/scss/**/*.scss')
    .pipe(sass())
    .pipe(gulp.dest('src/css'))
    .pipe(browserSync.reload({
      stream: true
    }))
});

gulp.task('serve', () => {
  browserSync.init({
    port: 8080,
    ghostMode: true,
    notify: false,
    server: 'src',
    open: true
  });
  
  gulp.watch('src/scss/**/*.scss', gulp.series('compileSass'));
  gulp.watch('src/*.html').on('change', browserSync.reload);
});

gulp.task('default', gulp.series(['serve']));