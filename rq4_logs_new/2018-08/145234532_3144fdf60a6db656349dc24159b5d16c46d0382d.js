var gulp = require('gulp');
var browserSync = require('browser-sync').create();
var pkg = require('./package.json');
var sass = require('gulp-sass');


// Copy third party libraries from /node_modules into /vendor
gulp.task('vendor', function() {

  // Bootstrap
   gulp.src([
      './node_modules/bootstrap/dist/**/*',
      '!./node_modules/bootstrap/dist/css/bootstrap-grid*',
      '!./node_modules/bootstrap/dist/css/bootstrap-reboot*'
    ])
    .pipe(gulp.dest('./src/vendor/bootstrap'));

  // jQuery
   gulp.src([
      './node_modules/jquery/dist/*',
      '!./node_modules/jquery/dist/core.js'
    ])
    .pipe(gulp.dest('./src/vendor/jquery'));

    // popper
    gulp.src(['node_modules/popper.js/dist/umd/popper.min.js'])
    .pipe(gulp.dest('src/vendor/popper'));

    //fontawesome
    gulp.src([
      'node_modules/@fortawesome/fontawesome-free/css/*'
    ])
    .pipe(gulp.dest('src/vendor/fontawesome'));

    //baguettebox
    gulp.src(['node_modules/baguettebox.js/dist/*.css','!./node_modules/baguettebox.js/dist/baguetteBox.css','node_modules/baguettebox.js/dist/*.js','!./node_modules/baguettebox.js/dist/baguetteBox.js'])
    .pipe(gulp.dest('./src/vendor/baguettebox'));

});

// sass
gulp.task('sass', function() {
    return gulp.src('src/scss/*.scss')
    .pipe(sass())
    .pipe(gulp.dest('src/css'))
    .pipe(browserSync.stream());
});


// Configure the browserSync task
gulp.task('browserSync',['sass','vendor'], function() {
  browserSync.init({
    server: {
      baseDir: "./src"
    }
  });
  gulp.watch('./src/scss/*.scss',['sass']);
  gulp.watch('./src/css/*.css', browserSync.reload);
  gulp.watch('./src/*.html', browserSync.reload);
});

// Default task
gulp.task('default',['browserSync']);
