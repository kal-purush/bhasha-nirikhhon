/* global require */

var gulp = require('gulp');
var sass = require('gulp-sass');

var config = {
	src: './app/sass/main.scss',
	dest: './app/css/',
	settings: {
		indentedSyntax: false, // Enable .sass syntax?
	}
};

gulp.task('sass', function() {
	gulp.src(config.src)
		.pipe(sass(config.settings))
		.pipe(gulp.dest(config.dest));
});