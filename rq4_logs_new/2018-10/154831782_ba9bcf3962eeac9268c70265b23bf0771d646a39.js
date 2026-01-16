var gulp = require('gulp');
var rename = require('gulp-rename');
var connect = require('gulp-connect');
var htmlmin = require('gulp-htmlmin');
var htmlincluder = require('gulp-htmlincluder');
var sass = require('gulp-sass');
var sourcemaps = require('gulp-sourcemaps');
var spritesmith = require('gulp.spritesmith');
var autoprefixer = require('gulp-autoprefixer');

gulp.task('server', function(){
    connect.server({
        root: 'build/',
        livereload: true,
        port: 5000
    });
});

gulp.task('html', function(){
    gulp.src('dev/**/*.html')
        .pipe(htmlincluder())
        // .pipe(htmlmin({
        //     collapseWhitespace: true,
        //     removeComments: 1
        // }))
        .pipe(rename(function(path){
            path.dirname = ''
        }))
        .pipe(gulp.dest('build/'))
        .pipe(connect.reload());
});

gulp.task('css', function(){
    gulp.src('dev/assets/sass/*.scss')
        .pipe(sourcemaps.init())
        .pipe(sass().on('error', sass.logError))
        .pipe(sourcemaps.write())
        .pipe(gulp.dest('build/'))
        .pipe(connect.reload());
});

gulp.task('move', function(){
    gulp.src('dev/assets/img/*.*')
        .pipe(rename(function(path){
            path.dirname = ''
        }))
        .pipe(gulp.dest('build/img/'));
    gulp.src('dev/assets/fonts/*.*')
        .pipe(gulp.dest('build/fonts/'));
    gulp.src('dev/assets/js/*.js')
        .pipe(gulp.dest('build/js/'));
});
gulp.task('sprite', function(){
    var sprite = gulp.src('dev/assets/img/sprite/*.*')
        .pipe(spritesmith({
            imgName: '../img/sprite.png',
            cssName: '_sprite.scss',
            padding: 5,
            algorithm: 'binary-tree'
        }));

        sprite.img.pipe(gulp.dest('build/img'));
        sprite.css.pipe(gulp.dest('dev/assets/scss/includes'));
});
gulp.task('pref', () =>
gulp.src('build/styles.css')
    .pipe(autoprefixer({
        browsers: ['last 2 versions'],
        cascade: false
    }))
    .pipe(gulp.dest('build'))
);

gulp.task('default', function(){
    gulp.start(['server', 'move', 'html', 'css']);

    gulp.watch(['dev/**/*.html'], function(){
        gulp.start(['html']);
    });

    gulp.watch(['dev/**/*.scss'], function(){
        gulp.start(['css']);
    });
});