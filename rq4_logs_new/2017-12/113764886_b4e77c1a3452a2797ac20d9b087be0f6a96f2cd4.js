const gulp = require('gulp');
const concat = require('gulp-concat');
const rename = require('gulp-rename');
const sass = require('gulp-sass');
const minifyCss = require('gulp-minify-css');
const sourcemaps = require('gulp-sourcemaps');
const babelify = require('babelify');
const browserify = require('browserify');
const source = require('vinyl-source-stream');
const buffer = require('vinyl-buffer');
const plumber = require('gulp-plumber');
const uglify = require('gulp-uglify');

gulp.task('default', ['styles', 'scripts']);
gulp.task('build', ['styles', 'scripts']);
gulp.task('styles', function (done) {
    gulp.src('./style/style.scss')
        .pipe(sourcemaps.init())
        .pipe(sass({
            errLogToConsole: true
        }))
        .pipe(minifyCss({
            keepSpecialComments: 0
        }))
        .pipe(rename({extname: '.min.css'}))
        .pipe(sourcemaps.write('.'))
        .pipe(gulp.dest('./dist'))
        .on('end', done)

});

gulp.task('scripts', function () {
    return browserify('./script/main.js')
        .transform(babelify.configure({
            presets: ['es2015']
        }))
        .bundle()
        .pipe(source('./script/main.js'))
        .pipe(buffer())
        .pipe(plumber())
        .pipe(sourcemaps.init({loadMaps: true}))
        .pipe(concat('main.min.js'))
        .pipe(uglify())
        .pipe(sourcemaps.write('.'))
        .pipe(gulp.dest('./dist'))
});

gulp.task('watch', ['default'], function () {
    gulp.watch(['./style/**/*.scss'], ['styles']);
    gulp.watch(['./script/**/*.js'], ['scripts'])
});