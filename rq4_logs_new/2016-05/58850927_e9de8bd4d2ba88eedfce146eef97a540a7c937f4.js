var gulp = require('gulp');
var shell = require('gulp-shell');

var paths = {
  'dist': './'
}

gulp.task('build', function() {
  return gulp.src(paths.dist, {read: false})
    .pipe(shell(['bash bin/zip.sh']));
});

gulp.task('default', ['build']);