var gulp = require('gulp');
var babel = require('gulp-babel');
var concat = require('gulp-concat');
var babelify = require('babelify');
var browserify = require('browserify');
var source = require('vinyl-source-stream');

gulp.task('jsx', function() {
    browserify({
      extensions: ['.jsx'],
      entries: './src/app/app.jsx',
      path: ['./src/app/', './node_modules'],
      debug: true
    })
    .transform(babelify)
    .bundle()
    .pipe(source('output.js'))
    .pipe(gulp.dest('./dest/js'));
});

gulp.task('css', function(){
  return gulp.src('./src/css/*.css').
  pipe(concat('all.css')).
  pipe(gulp.dest('./dest/css'));
});

gulp.task("move-css", function(){
  return gulp.src([
    './vendor/ratchet/css/*.css'
  ]).
  pipe(gulp.dest('./dest/css'));
});

gulp.task("move-fonts", function(){
  return gulp.src([
    './vendor/ratchet/fonts/*.{eot,svg,ttf,woff}'
  ]).
  pipe(gulp.dest('./dest/fonts'));
});

gulp.task("move-js", function(){
  return gulp.src([
    './vendor/ratchet/js/*.js',
    './node_modules/react/dist/react-with-addons.js',
    './vendor/fastclick/lib/fastclick.js',
    './vendor/hammer/hammer.min.js',
    './node_modules/react-router/umd/ReactRouter.js'
  ]).
  pipe(gulp.dest('./dest/js'));
});

gulp.task('move', ['move-js', 'move-fonts', 'move-css']);

gulp.task("index", function(){
  return gulp.src('./src/index.html').
  pipe(gulp.dest('./dest'));
});

gulp.task("watch", function(){
  gulp.watch(['./src/app/*', './src/app/**/*'], ['jsx']);
  gulp.watch('./src/index.html', ['index']);
  gulp.watch('./src/css/*.css', ['css']);
});

gulp.task("default", ['jsx', 'index', 'move', 'css', 'watch']);