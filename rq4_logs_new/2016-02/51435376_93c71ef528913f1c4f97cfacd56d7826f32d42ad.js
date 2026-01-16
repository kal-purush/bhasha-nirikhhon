/* Get Bower files */
var gulp = require('gulp'),
    config = require('./config'),
    uglify = require('gulp-uglify'),
    sourcemaps = require('gulp-sourcemaps'),
    gulpFilter = require('gulp-filter'),
    concat = require('gulp-concat'),
    gutil = require('gulp-util'),
    mainBowerFiles = require('main-bower-files'),
    rename = require('gulp-rename'),
    browserSync = require("browser-sync"),
    reload = browserSync.reload,
    plumber = require('gulp-plumber');



gulp.task('bower', function () {
    var jsFilter = gulpFilter(['**/*.js'], {restore: true}),
        cssFilter = gulpFilter(['**/*.{css,less}'], {restore: true});

    return gulp.src(mainBowerFiles({
        includeDev: true
    }))
        .pipe(plumber(function(error) {
            gutil.log(gutil.colors.red(error.message));
            this.emit('end');
        }))
        // Get vendor JavaScript
        .pipe(jsFilter)
        .pipe(sourcemaps.init())
        .pipe(gulp.dest(config.pathTo.Src.BowerJSVendor))
        .pipe(concat('vendor-bundle.js'))
        .pipe(gulp.dest(config.pathTo.Src.BowerJSVendor))
        .pipe(rename({ suffix: '.min' }))
        .pipe(uglify())
        .pipe(sourcemaps.write('./'))
        .pipe(gulp.dest(config.pathTo.Src.BowerJSVendor))
        .pipe(jsFilter.restore)
        // Get vendor CSS
        .pipe(cssFilter)
        .pipe(gulp.dest(config.pathTo.Src.CSSVendor))
        .pipe(reload({stream: true}));
});