// File: Gulpfile.js
'use strict';
var gulp = require('gulp'),
    del = require('del'),
    inject = require('gulp-inject'),
    angularFilesort = require('gulp-angular-filesort'),
    webserver = require('gulp-webserver'),
    jshint = require('gulp-jshint'),
    minifyCss = require('gulp-minify-css'),
    uglify = require('gulp-uglify'),
    stylish = require('jshint-stylish'),
    gulpif = require('gulp-if'),
    useref = require('gulp-useref'),
    minifyHtml = require('gulp-minify-html'),
    rev = require('gulp-rev'),
    sass = require('gulp-sass'),
    uncss = require('gulp-uncss'),
    imagemin = require('gulp-imagemin'),
    pngcrush = require('imagemin-pngcrush');

// Borra el directorio dist
gulp.task('clean', function () {
      del('./dist');
      return del('./tmp');
    });

// Servidor web para probar el entorno de producción

gulp.task('server-dist', function() {
  gulp.src('./dist/')
    .pipe(webserver({
      livereload: true,
      directoryListing: false,
      open: true
    }));
});

// Busca errores en el JS y nos los muestra en el terminal

gulp.task('jshint', function() {
   return gulp.src('./src/js/**/*.js')
    .pipe(jshint('.jshintrc'))
    .pipe(jshint.reporter('jshint-stylish'))
    .pipe(jshint.reporter('fail'));
});

gulp.task('sass', ['clean'], function() {
    return gulp.src('./src/scss/libertual-style.scss')
        .pipe(sass({outputStyle: 'compressed'}).on('error', sass.logError))
        .pipe(gulp.dest('./tmp/css/'));
});

// Busca en las carpetas de estilos y javascript los archivos
// para inyectarlos en el index.html

gulp.task('inject', ['sass'], function() {
  gulp.src('./src/js/**/*.js')
    .pipe(gulp.dest('./tmp/js'));
  return gulp.src('./src/index.html')
  .pipe(inject(gulp.src('./src/js/libertual-*.js'), {starttag: '<!-- inject:app:{{ext}} -->', ignorePath: '/src'}))
  .pipe(inject(
    gulp.src(['./src/js/vendor/*.js']).pipe(angularFilesort()), {
      read: false,
      ignorePath: '/src'
    }))
  .pipe(inject(
    gulp.src(['./tmp/css/*.css']), {
      read: false,
      ignorePath: '/tmp'
    }))
  .pipe(gulp.dest('./tmp'));
});

// Comprime los archivos CSS y JS enlazados en el index.html
// y los minifica.

gulp.task('compress', function() {
  gulp.src('./tmp/index.html')
    .pipe(useref.assets())
    .pipe(gulpif('*.js', uglify({mangle: false })))
    .pipe(gulp.dest('./dist'));
});

// Elimina el CSS que no es utilizado para reducir el pesodel archivo
gulp.task('uncss', function() {
  gulp.src('./dist/css/style.min.css')
    .pipe(uncss({
      html: ['./tmp/index.html']
    }))
    .pipe(gulp.dest('./dist/css'));
});

// Copia el contenido de los estáticos e index.html al directorio
// de producción sin tags de comentarios

gulp.task('copy', function() {
  gulp.src('./tmp/index.html')
    .pipe(useref())
    .pipe(gulp.dest('./dist'));
  gulp.src('./src/resume.json')
    .pipe(gulp.dest('./dist'));
});

// Copia y optimiza las imagenes

gulp.task('images', function() {
  gulp.src('./src/images/*.{png,jpg,jpeg,gif,svg}')
    .pipe(imagemin({
      progressive: true,
      svgoPlugins: [{removeViewBox: false}],
      use: [pngcrush()]
    }))
    .pipe(gulp.dest('./dist/images'));
});

gulp.task('watch', function() {
  gulp.watch(['./src/scss/**/*.scss'], ['inject','copy']);
  gulp.watch(['./src/js/**/*.js', './Gulpfile.js'], ['jshint', 'copy', 'compress']);
});

gulp.task('default', ['inject']);
gulp.task('build', ['copy', 'compress', 'images']);
gulp.task('server', ['watch','server-dist']);