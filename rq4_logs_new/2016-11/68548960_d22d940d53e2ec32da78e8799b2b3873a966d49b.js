var gulp = require('gulp')
var babel = require('gulp-babel')
var del = require('del')
var stylus = require('gulp-stylus')
var image = require('gulp-image')
var browserSync = require('browser-sync').create()

var src = './public/src'
var dist = './public/dist'

//noinspection SpellCheckingInspection
var paths = {
    scripts: [src + '/js/**/*.js'],
    styles: [src + '/styles/**/*.styl'],
    images: [src + '/images/**/*.png', src + '/images/**/*.jpg', src + '/images/**/*.gif', src + '/images/**/*.jpeg']
}

gulp.task('clean', function () {
    return del([dist])
})

gulp.task('scripts', function () {
    //noinspection SpellCheckingInspection
    var res = gulp.src(paths.scripts)
        .pipe(babel({
            comments: false,
            compact: true,
            minified: true
        }))
        .pipe(gulp.dest(dist + '/js'))
    del(dist + '/js/*-compiled.js')
    return res
})

gulp.task('styles', function () {
    return gulp.src(paths.styles).pipe(stylus({ compress: true })).pipe(gulp.dest(dist + '/css'))
})

gulp.task('images', function () {
    //noinspection SpellCheckingInspection
    return gulp.src(paths.images).pipe(image({
        pngquant: true,
        optipng: false,
        zopflipng: true,
        jpegRecompress: false,
        jpegoptim: true,
        mozjpeg: true,
        gifsicle: true,
        svgo: true,
        concurrent: 10
    })).pipe(gulp.dest(dist + '/images'))
})

for (var task in paths)
    gulp.task('watch-' + task, [task], function (done) {
        browserSync.reload()
        done()
    })

gulp.task('serve', ['scripts', 'styles', 'images'], function () {
    for (var task in paths)
        for (var i = 0; i < paths[task].length; i++)
            gulp.watch(paths[task][i], ['watch-' + task])
})

gulp.task('default', ['serve'])