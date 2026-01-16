var gulp        = require('gulp'),
	browserSync = require('browser-sync').create(),
    jekyll = require('gulp-jekyll'),
	sass = require('gulp-sass');


var reload  = browserSync.reload;

gulp.task('sass', function() {
    gulp.src(['./assets/scss/*.scss','./assets/scss/**/*.scss'])
        .pipe(sass().on('error', sass.logError))
        .pipe(gulp.dest('./assets/'));
});

gulp.task('browser-sync', function() {
    browserSync.init({
        port: 8080,
        open: false,
        notify: false,
        server: {
            baseDir: "./_site"
        }
    });
});

gulp.task('jekyll', function () {

    gulp.src(['./index.html'])
        .pipe(jekyll({
            source: './',
            bundleExec: true
        }))
        .pipe(gulp.dest('./_site/'));

});

gulp.task('default', ['browser-sync'], function () {

    gulp.watch(['./index.html', './_includes/*.html'], ['jekyll', reload]);
    gulp.watch('./assets/scss/**/*.scss',['sass', reload]);
});