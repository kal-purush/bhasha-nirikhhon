var gulp = require('gulp');
var watch = require('gulp-watch');
var less = require('gulp-less');
var path = require('path');

gulp.task('less', function () {
  return gulp.src('./src/spacer.less')
    .pipe(less({
      paths: [ path.join(__dirname, 'less', 'includes') ]
    }))
    .pipe(gulp.dest('./dist'));
});

gulp.task('watch', function() {
    gulp.watch('./src/spacer.less', ['less']); 
});

gulp.task('default', ['less','watch']);