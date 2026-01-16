var gulp = require('gulp');
var sass = require('gulp-sass');
var sourcemaps = require('gulp-sourcemaps');
var image = require('gulp-image');
var watch = require('gulp-watch');

gulp.task('sass', function () {
    'use strict';
    gulp.src('./src/MainBundle/Resources/sass/*.scss')
        .pipe(watch('./src/MainBundle/Resources/sass/*.scss'))
        .pipe(sourcemaps.init())
        .pipe(sass())
        .pipe(sourcemaps.write('./'))
        .pipe(gulp.dest('./web/bundles/main/css'));
});

gulp.task('sass-deliver', function(){
    'use strict';
    gulp.src('./src/MainBundle/Resources/sass/*.scss')
        .pipe(sass())
        .pipe(gulp.dest('./web/bundles/main/css'));
});

gulp.task('image', function () {
    'use strict';
    gulp.src('./src/MainBundle/Resources/images/*')
    .pipe(image())
    .pipe(gulp.dest('./web/bundles/main/images/'));
});


gulp.task('build',['sass-deliver']);