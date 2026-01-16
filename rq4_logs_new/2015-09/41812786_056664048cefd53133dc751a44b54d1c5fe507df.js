var fs = require('fs'),
    gulp = require('gulp'),
    eslint = require('gulp-eslint')
    Karma = require('karma').Server;
;

var paths = {
  src: 'src/**/*.js',
  test: 'spec/**/*Spec.js'
};

gulp.task('dev-lint', function() {
  return gulp.src([paths.src, paths.test])
    .pipe(eslint())
    .pipe(eslint.format())
    .pipe(eslint.failOnError());
});

gulp.task('ci-lint', function() {
  return gulp.src([paths.src, paths.test])
    .pipe(eslint())
    .pipe(eslint.format('checkstyle', function(data) { fs.writeFileSync('checkstyle.xml', data, 'utf8'); }))
});

gulp.task('dev-karma', function(done) {
  new Karma({
    configFile: __dirname + '/karma.conf.js',
    singleRun: false,
    reporters: [ 'progress', 'coverage' ],
    coverageReporter: { type: 'html' },
    browsers: [ 'Chrome' ],
  }, done).start();
});

gulp.task('ci-karma', function(done) {
  new Karma({
    configFile: __dirname + '/karma.conf.js',
    singleRun: true,
    reporters: [ 'junit', 'coverage' ],
    coverageReporter: { type: 'cobertura' },
    browsers: [ 'Chrome', 'Firefox' ],
  }, done).start();
});