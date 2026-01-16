var gulp = require("gulp");
    gulp.task('install', function () {
        return gulp.src(["./Views/**/*.*", "./wwwroot/**/*.*", "./tasks/**/*.*"], { base: '.' })
    .pipe(gulp.dest("../.."));
    });