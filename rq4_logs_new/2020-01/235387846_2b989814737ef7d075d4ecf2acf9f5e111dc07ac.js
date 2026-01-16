const gulp = require('gulp')
const browserSync = require('browser-sync').create();
const watch = require('gulp-watch')
const sass = require('gulp-sass')

// Gaulp Task  для компиляции scss
gulp.task('scss', function () {
    return gulp
        .src('./src/scss/main.scss')
        .pipe(sass())
        .pipe(gulp.dest('./src/css/'))
        .pipe(browserSync.stream())
});

// Gulp Task для поднятия сервера локально
gulp.task('browser-sync', function () {
    browserSync.init({
        server: {
            baseDir: "./src/"
        }
    });
});
//  следим за файлами и обнавляем браузел
gulp.task('watch', function () {
    watch(['./src/*.html', './src/css/*.css', './src/js/*.js', './src/img/*.*'], gulp.parallel(browserSync.reload));
    // watch('./src/*.css', gulp.parallel(browserSync.stream));
    // наблюдать за всеми scss файлами на любом уровне сложности
    watch('./src/scss/**/*scss', function () {
        setTimeout(gulp.parallel('scss'), 1000)
    })
    // watch('./app/css/*.css').on("change", browserSync.reload);
});

gulp.task('default', gulp.series(
    'scss',
    gulp.parallel('browser-sync', 'watch'))
);



// создаём первый Gulp Task
// gulp.task('hello', function (cb) {
//     console.log('Hello, from Gulp');
//     cb()
// })


// gulp.task('goodbye', function (cb) {
//     console.log('\n', '===== Goodbye, from Gulp =====', '\n');
//     cb()
// })




// // задача по умолчанию - запускаектся без набора имени задачи
// gulp.task('default', function (cb) {
//     console.log('==== Defult Gulp task =====');
//     cb()
// })

//  последовательное выполнение задач
// gulp.task('default', gulp.series('hello', 'goodbye')
// )

// Параллельное выполнение задач:

// gulp.task('para', gulp.parallel ('hello', 'goodbye'))
