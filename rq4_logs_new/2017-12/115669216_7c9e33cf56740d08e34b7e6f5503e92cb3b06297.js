var gulp = require('gulp');
var ts = require('gulp-typescript');
var tsProject = ts.createProject('tsconfig.json');

var tslint = require('gulp-tslint');

var colors = require('ansi-colors');

var log = require('fancy-log');

var exec = require('child_process').exec;

gulp.task('build', ['tslint'], () => 
  tsProject.src()
    .pipe(tsProject())
    .on('error', () => { process.exit(1) })
    .js.pipe(gulp.dest('dist'))
);

gulp.task('tslint', () =>
  tsProject.src()
    .pipe(tslint({
    // write a custom formatter for this, so that I can make it as prose-like yellow warnings
      formatter: 'stylish'
    }))
    .pipe(tslint.report({
      allowWarnings: true,
      emitError: false,
      fix: false,
      summarizeFailureOutput: true
    }))
);

gulp.task('default', ['tslint', 'build']);

gulp.task('start', ['tslint', 'build',], (callback) =>
  exec('npm start', (err, stdout, stderr) => {
    if (stderr) {
      callback('Electron start failure')
      log.error(colors.red(stderr));
      process.exit(2);
    }
  })
);