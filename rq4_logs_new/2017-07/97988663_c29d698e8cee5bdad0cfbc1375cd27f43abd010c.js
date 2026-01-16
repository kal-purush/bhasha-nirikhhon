var gulp = require('gulp'),
		browserify = require('gulp-browserify');
		concat = require('gulp-concat'),
		uglify = require('gulp-uglify'),
		plumber = require('gulp-plumber'),
		nodemon = require('gulp-nodemon'),
		gutil = require('gulp-util');

// Compile bowerify js
gulp.task('scripts', () => {
	// Single entry point to browserify
	gulp.src('public/javascripts/view/*.js')
		.pipe(plumber())
		.pipe(concat('view.js'))
		.pipe(browserify({
			insertGlobals : true,
			debug : !gutil.env.production
		}))
		.pipe(uglify())
		.pipe(plumber.stop())
		.pipe(gulp.dest('./public/build'))
});

// Run the server
gulp.task('server', () => {
	nodemon({
		// the script to run the app
		script: 'bin/www',
		// listen to
		watch: ["bin/www", "app.coffee", "routes/", "models/", "auth/",
		"public/javascripts/", "public/javascripts/view/", "gulpfile.js"],
		// extensions
		ext: 'js coffee',
		// tasks
		tasks: ['scripts']
	}).on('restart', () => {
	gulp.src('bin/www')
	});
});

// Set default task
gulp.task('default', ['scripts','server']);