var gulp = require('gulp');
var sass = require('gulp-sass');

function styles() {
    return gulp.src('src/scss/**/*.scss')
        .pipe(sass().on('error', sass.logError))
        .pipe(gulp.dest('./assets/css/'));
}

//Watch task
gulp.task('default', function () {
    gulp.watch('src/scss/**/*.scss', styles);
});