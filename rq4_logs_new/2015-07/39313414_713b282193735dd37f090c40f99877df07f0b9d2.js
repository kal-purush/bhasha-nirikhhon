var gulp = require('gulp');
var sass = require('gulp-sass');
var watch = require('gulp-watch');

gulp.task('compile:sass', function(){
    gulp.src('css/*.scss')
        .pipe(sass())
        .pipe(gulp.dest('./css'));
});

gulp.task('watch:sass', function() {
    gulp.watch('css/*.scss', ['compile:sass']);
})