const gulp = require('gulp');
const mocha = require('gulp-mocha');
const eslint = require('gulp-eslint');

gulp.task('lint', () =>
    gulp.src(['**/*.js', '!node_modules/**', '!gulpfile.js'])
        .pipe(eslint())
        .pipe(eslint.format())
        .pipe(eslint.failAfterError())
);

gulp.task('mocha', () =>
    gulp.src(['test/*.js'], {read: false})
        .pipe(mocha())
);

gulp.task('default', ['lint', 'mocha']);