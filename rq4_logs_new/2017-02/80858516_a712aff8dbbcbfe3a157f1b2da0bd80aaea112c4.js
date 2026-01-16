var gulp = require('gulp');
var sass = require('gulp-sass');
var connect = require('gulp-connect');

gulp.task('connect', function(){
  connect.server({
    root: '/',
    livereload: true
  });
});

gulp.task('sass', function () {
  return gulp.src('./scss/*.scss')
      .pipe(sass({ 
        errLogToConsole: true,
        sourceComments: true,
        includePaths: ['bower_components/foundation/scss']
      }).on('error', sass.logError))
      .pipe(gulp.dest('./css'));
});

gulp.task('livereload', function (){
  gulp.src('./**/*')
  .pipe(connect.reload());
});

gulp.task('watch', function () {
  gulp.watch('./scss/**/*.scss', ['sass']);
  gulp.watch('./**/*', ['livereload']);
});

gulp.task('default', ['connect', 'watch', 'sass']);