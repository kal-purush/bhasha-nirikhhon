const { series, parallel, src, dest } = require('gulp')

const gulp = require('gulp'),
    babel = require('gulp-babel'),
    concat = require('gulp-concat'),
    browserSync = require('browser-sync').create()

gulp.task('css', function () {
    return src('./src/css/*.css')
        .pipe(concat('main.css'))
        .pipe(dest('./dist'))
})

gulp.task('js', function () {
    return src('./src/js/*.js')
        .pipe(gulp.dest('./dist/js'))
})

gulp.task('html', function () {
    return gulp.src('./src/index.html')
        .pipe(gulp.dest('./dist'))
})


gulp.task('serve', function () {
    browserSync.init({
        server: {
            baseDir: 'dist',
        },
    })

    gulp.watch('./src/js/*.js').on('change', series('js'))
    gulp.watch('./src/index.html').on('change', series('html'))

    gulp.watch('./dist/main.css').on('change', browserSync.reload)
    gulp.watch('./dist/index.html').on('change', browserSync.reload)
    gulp.watch('./dist/script.js').on('change', browserSync.reload)
})

gulp.task(
    'build',
    series(
        'js',
        parallel('html', 'css')
    )
)

gulp.task(
    'default',
    series(
        'js',
        parallel('html', 'css'),
        'serve'
    )
)