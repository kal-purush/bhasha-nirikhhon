const { src, dest, watch, parallel, series } = require('gulp');

const scss  = require('gulp-sass')(require('sass'));
const concat = require('gulp-concat');
const browserSync = require('browser-sync').create();
const uglify = require('gulp-uglify-es').default;
const autoprefixer = require('gulp-autoprefixer');
const imagemin = require('gulp-imagemin');
const del = require('del');

function browsersync() {
    browserSync.init({
        server : {
            baseDir: 'app/'
        }
    });
}

function cleanBuild() {
    return del('build')
}

function images() {
    return src('app/images/src/**/*')
    .pipe(imagemin())
    .pipe(dest('app/images/dest/'))
}

function scripts() {
    return src([
        'app/js/main.js'
    ])
    .pipe(concat('main.min.js'))
    .pipe(uglify())
    .pipe(dest('app/js'))
    .pipe(browserSync.stream())
}

function styles() {
    return src('app/scss/style.scss')
        .pipe(scss({outputStyle: 'compressed'}))
        .pipe(concat('style.min.css'))
        .pipe(autoprefixer({
            overrideBrowserslist: ['last 10 version'],
            grid: true
        }))
        .pipe(dest('app/css/'))
        .pipe(browserSync.stream())
}
function build() {
    return src([
       'app/css/**/*.min.css',
       'app/js/**/*.min.js',
       'app/images/dest/**/*',
       'app/**/*.html',
   ], {base: 'app'})
   .pipe(dest('build'));
}

function watching() {
    watch(['app/scss/**/*.scss'], styles);
    watch(['app/js/**/*.js', '!app/js/main.min.js'], scripts);
    watch(['app/*.html']).on('change', browserSync.reload);
    watch('app/images/src/**/*', images);
}

exports.styles = styles;
exports.watching = watching; 
exports.browsersync = browsersync;
exports.scripts = scripts;
exports.images = images;
exports.cleanBuild = cleanBuild;

exports.build = series(cleanBuild, images, build)
exports.default = parallel(styles, scripts, browsersync, watching)