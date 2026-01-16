var gulp = require('gulp'),
browserSync = require('browser-sync').create(),
reload = browserSync.reload,
sass = require('gulp-sass'),
watch = require('gulp-watch'),
px2rem = require('gulp-px2rem'),
cssmin = require('gulp-minify-css');

gulp.task('browser-sync', ['sass'], function(){
  browserSync.init({
    server:{
    	baseDir:'./demo'
    }
  });
  
  gulp.watch('./src/*.scss', [ 'sass' ]);
});

gulp.task('sass', function(){
	return gulp.src('./src/*.scss')
      .pipe( sass() )
    	.pipe( gulp.dest( './demo' ) )
      .pipe( browserSync.stream() );
});