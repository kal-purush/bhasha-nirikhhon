var gulp        = require("gulp"),
    jshint      = require("gulp-jshint")
    browserSync = require("browser-sync"),
    typescript  = require("gulp-typescript"),
    clean       = require("gulp-clean"),
    concat      = require("gulp-concat"),
    stripDebug  = require("gulp-strip-debug"),
    uglify      = require("gulp-uglify"),
    uglifyCSS   = require("gulp-uglifycss")
    bowerFiles  = require("main-bower-files"),
    runSequence = require("run-sequence");

gulp.task('hello', function() {
    console.log('This is "Gulp" signing in...');
});

gulp.task('cleanBuildFolder', function() {
    return gulp.src('./build', {read: false})
            .pipe(clean());
});

gulp.task('jsHint', function() {
    gulp.src('./src/js/*.js')
        .pipe(jshint())
        .pipe(jshint.reporter('default'));
});

gulp.task('copyHTML', function() {
    gulp.src('./src/**/*.html')
        .pipe(gulp.dest('./build'))
        .pipe(browserSync.reload({stream: true}));
});

gulp.task('copyCSS', function(){
    gulp.src('./src/css/fonts/**/*.*')
        .pipe(gulp.dest('./build/css/fonts'))
    gulp.src('./src/css/*.css')
        .pipe(concat('main.css'))
        .pipe(uglifyCSS())
        .pipe(gulp.dest('./build/css'))
        .pipe(browserSync.reload({stream: true}));
})
gulp.task('copyJS',  function() {
    gulp.src(['./src/js/test.js','src/js/app.js'])
        .pipe(concat('scripts.min.js'))
        //.pipe(stripDebug())
        .pipe(uglify())
        .pipe(gulp.dest('./build/js'))
        .pipe(browserSync.reload({stream: true}));
})

gulp.task('copyLib', function() {
    return gulp.src(bowerFiles(), {base:'./bower_components'})
        .pipe(concat('lib.min.js'))
        .pipe(uglify())
        .pipe(gulp.dest('./build/js'))
        .pipe(browserSync.reload({stream: true}));
})

gulp.task('copyClasses', function() {
    gulp.src(['./src/js/classes/*.ts'])
        .pipe(typescript())
        .pipe(concat("classes.min.js"))
        .pipe(uglify())
        .pipe(gulp.dest('./build/js'))
        .pipe(browserSync.reload({stream: true}));
});

gulp.task('browserSync', function() {
    browserSync({ server: {baseDir: './build'}});
});

gulp.task('watchFiles', function() {
    gulp.watch('./src/**/*.html', ['copyHTML']);
    gulp.watch('./src/css/**/*.css', ['copyCSS']);
    gulp.watch('./src/js/**/*.js', ['copyJS']);
    gulp.watch('./src/classes/**/*.ts', ['copyClasses']);
});

gulp.task('default', function() {
    runSequence(
        ['cleanBuildFolder'],
        ['copyHTML', 'copyCSS', 'copyJS', 'copyClasses', 'copyLib'],
        ['browserSync'],
        ['watchFiles']);
});