
var gulp = require('gulp');
var changed = require('gulp-changed');
var config = require('../config');

gulp.task('html', function() {
    console.log('temp');

    return gulp.src('app/index.html')
        .pipe(changed(config.dev.root))
        .pipe(gulp.dest(config.dev.root));

});