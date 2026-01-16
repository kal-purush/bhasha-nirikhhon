var gulp = require('gulp'),
    del = require('del'),
    runseq = require('run-sequence'),
    concat = require('gulp-concat'),
    sourcemaps = require('gulp-sourcemaps');

gulp.task('clean', function () {
    return del('build');
});

gulp.task('scripts', function () {
    return gulp.src([
            'src/libs/ol3/ol-debug.js',
            'src/map/map.js'
        ])
        .pipe(sourcemaps.init())
        .pipe(concat('script.js'))
        .pipe(sourcemaps.write())
        .pipe(gulp.dest('build/'));
});

gulp.task('styles', function () {
    return gulp.src([
            'src/libs/*/*.css',
            'src/style/*.css'
        ])
        .pipe(sourcemaps.init())
        .pipe(concat('style.css'))
        .pipe(sourcemaps.write())
        .pipe(gulp.dest('build/'));
});

gulp.task('build', ['clean'], function (cb) {
    runseq(['styles', 'scripts'], cb);
});

gulp.task('watch', function () {
    gulp.watch('src/style/*.css', ['styles']);
    gulp.watch('src/**/*.js', ['scripts']);
});

gulp.task('default', ['build', 'watch'], function () {
    var express = require('express'),
        server = express(),
        port = 9090;

    server.use(express.static(__dirname));
    server.listen(port);

    console.log('Webserver started with port ' + port);
});