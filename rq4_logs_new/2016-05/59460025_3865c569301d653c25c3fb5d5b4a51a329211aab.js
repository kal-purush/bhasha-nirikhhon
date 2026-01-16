let browserSync = require("browser-sync").create();
const gulp = require('gulp');
const babelify = require('babelify');
const browserify = require('browserify');
const watchify = require('watchify');
const gutil = require("gulp-util");
const notify = require('gulp-notify');
const source = require("vinyl-source-stream");
const buffer = require('vinyl-buffer');

gulp.task("watch", function() {
  var b = browserify({
    entries: ['scripts/index.js']
  }).transform(babelify);

  function bundle() {
    b.bundle()
      .on("log", gutil.log)
      .on("error", notify.onError())
      .pipe(source('main.js'))
      .pipe(buffer())
      .pipe(gulp.dest('dist'))
      .pipe(browserSync.reload({
        stream: true
      }));
  }

  watchify(b, {
    delay: 60
  }).on("update", bundle);
  bundle();
});

gulp.task("serve", function() {
  browserSync.init({
    server: {
      baseDir: "./"
    }
  });
});

gulp.task("default", ["watch", "serve"]);