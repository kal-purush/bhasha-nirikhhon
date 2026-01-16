var gulp = require('gulp'),
  sass = require('gulp-ruby-sass'),
  autoprefixer = require('gulp-autoprefixer'),
  minifycss = require('gulp-minify-css'),
  jshint = require('gulp-jshint'),
  uglify = require('gulp-uglify'),
  rename = require('gulp-rename'),
  concat = require('gulp-concat'),
  notify = require('gulp-notify'),
  cache = require('gulp-cache'),
  livereload = require('gulp-livereload'),
  del = require('del'),
  source = require('vinyl-source-stream'),
  browserify = require('browserify'),
  watchify = require('watchify'),
  reactify = require('reactify'),
  concat = require('gulp-concat'),
  sourcemaps = require('gulp-sourcemaps');

gulp.task('clean', function(cb) {
  del(['build/**/*'], cb)
});

gulp.task('html', function() {
    gulp.src('./src/index.html')
    .pipe(gulp.dest('./build'));
});

gulp.task('browserify', function() {
  var bundler = browserify({
    entries: ['./src/js/main.js'],
    transform: [reactify],
    debug: true,
    cache: {}, packageCache: {}, fullPaths: true
  });
  var watcher  = watchify(bundler);

  return watcher
  .on('update', function () {
    var updateStart = Date.now();
    console.log('Updating!');
    watcher.bundle()
    .pipe(source('main.js'))
    .pipe(gulp.dest('./build/'));
    console.log('Updated!', (Date.now() - updateStart) + 'ms');
  })
  .bundle()
  .pipe(source('main.js'))
  .pipe(gulp.dest('./build/'));
});

gulp.task('sass', function () {
  return sass('src/styles/main.scss')
    .on('error', function (err) {
      console.error('Error!', err.message);
    })
    .pipe(autoprefixer('last 2 version'))
    .pipe(concat('main.css'))
    .pipe(sourcemaps.write())
    .pipe(gulp.dest('build'));
});

gulp.task('scripts', function() {
  return gulp.src('src/js/**/*.js')
    .pipe(jshint('.jshintrc'))
    .pipe(jshint.reporter('default'))
    .pipe(concat('main.js'))
    .pipe(gulp.dest('build'))
    .pipe(rename({suffix: '.min'}))
    .pipe(uglify())
    .pipe(gulp.dest('build'))
    .pipe(notify({ message: 'Scripts task complete' }));
});

gulp.task('copy-normalize', function() {
   gulp.src('./bower_components/normalize.css/normalize.css')
   .pipe(gulp.dest('./build'));
});

gulp.task('copy-fonts', function() {
  return gulp.src(['./src/assets/*'])
  .pipe(gulp.dest('./build/assets'));
});

gulp.task('watch', function() {
  gulp.watch('src/**/*.html', ['html']);
  gulp.watch('src/styles/**/*.scss', ['sass']);
  gulp.watch('src/scripts/**/*.js', ['scripts']);
  livereload.listen();
  gulp.watch(['build/**']).on('change', livereload.changed);
});


gulp.task('dev', ['clean'], function() {
  gulp.start('html', 'sass', 'browserify', 'copy-fonts', 'watch');
});