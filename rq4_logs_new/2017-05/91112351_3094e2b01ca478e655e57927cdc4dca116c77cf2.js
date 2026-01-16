'use strict';

/*gulp.task('sass:watch', function () {
  gulp.watch('./sass/!**!/!*.scss', ['sass']);
});*/


var gulp = require('gulp'),
    sass = require('gulp-sass'),
    iconfont = require('gulp-iconfont'),
    iconfontCss = require('gulp-iconfont-css'),
    fontName = 'icons',
    autoprefixer = require('gulp-autoprefixer');


gulp.task('icons', function(){
    return gulp.src(['icons/*.svg'])
        .pipe(iconfontCss({
            path: 'sass/_icons-template.scss',
            fontName: fontName,
            targetPath: '../sass/_icons.scss',
            fontPath: '../fonts/'
        }))
        .pipe(iconfont({
            fontName: fontName
        }))
        .pipe(gulp.dest('fonts'));
});

gulp.task('sass', ['icons'], function () {
    return gulp.src('sass/**/*.scss')
        .pipe(sass().on('error', sass.logError))
        .pipe(autoprefixer())
        .pipe(gulp.dest('./css'));
});

gulp.task('default', ['icons', 'sass']);