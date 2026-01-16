/// <reference path="typings/tsd.d.ts" />

import gulp = require('gulp');
import clean = require('gulp-clean');

gulp.task('clean', (done) => {
	gulp.src('spec/**/*.js', { read: false })
		.pipe(clean());
	
	gulp.src('src/**/*.js', { read: false })
		.pipe(clean());
		
	gulp.src('*.js', {read: false })
		.pipe(clean());
		
	gulp.src('dist', { read: false })
		.pipe(clean());
		
	done();
});

gulp.task('dist', (done) => {
	gulp.src('src/**/*.js')
		.pipe(gulp.dest('dist/app'));
		
	gulp.src('css/**/*.*')
		.pipe(gulp.dest('dist/css'));
		
	gulp.src('node_modules/bootstrap/dist/css/bootstrap.css')
		.pipe(gulp.dest('dist/vendor/bootstrap/css'));
		
	gulp.src('node_modules/bootstrap/dist/js/bootstrap.js')
		.pipe(gulp.dest('dist/vendor/bootstrap/js'));
		
	gulp.src('node_modules/bootstrap/dist/fonts/*.*')
		.pipe(gulp.dest('dist/vendor/bootstrap/fonts'))
		
	gulp.src('index.html')
		.pipe(gulp.dest('dist'));
		
	gulp.src('main.js')
		.pipe(gulp.dest('dist'));
		
	gulp.src('package.json')
		.pipe(gulp.dest('dist'));
});

gulp.task('default', () => {
	console.log('Oki!');
});