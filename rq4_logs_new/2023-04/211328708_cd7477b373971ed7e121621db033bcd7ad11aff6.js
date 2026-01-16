const gulpEsLint = require('gulp-eslint');
const gulp = require('gulp');

gulp.task('default', function() {
	return gulp.src('./*.js')
	.pipe(gulpEsLint.format());
});