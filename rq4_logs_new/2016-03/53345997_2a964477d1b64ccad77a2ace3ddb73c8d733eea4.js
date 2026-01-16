import gulp from 'gulp';
import eslint from 'gulp-eslint';
import uglify from 'gulp-uglify';
import sourcemaps from 'gulp-sourcemaps';
import source from 'vinyl-source-stream';
import buffer from 'vinyl-buffer';
import browserify from 'browserify';
import watchify from 'watchify';
import babel from 'babelify';

function logErrors(func) {
  return (...args) => {
    try {
      func(...args);
    } catch (e) {
      console.error('===== ERROR =====', "\n", e);
    }
  }
}

function compile(watch) {
  var bundler = watchify(
    browserify('./src/index.js', { debug: true })
      .transform(babel)
  );

  const rebundle = () => {
    bundler.bundle()
      .on('error', (e) => { console.error(e); this.emit('end'); })
      .pipe(source('build.js'))
      .pipe(buffer())
      .pipe(uglify())
      .pipe(sourcemaps.init({ loadMaps: true }))
      .pipe(sourcemaps.write('./'))
      .pipe(gulp.dest('./public/scripts'));
  };

  if (watch) {
    bundler.on('update', () => { console.log('-> bundling...'); rebundle(); });
  }

  rebundle();
}

function watch() {
  return compile(true);
}

function lint() {
  return gulp.src('src/**')
    .pipe(eslint())
    .pipe(eslint.format())
    .pipe(eslint.failOnError());
}

gulp.task('build', compile);
gulp.task('lint', lint)
gulp.task('watch', logErrors(watch));
gulp.task('default', ['watch']);