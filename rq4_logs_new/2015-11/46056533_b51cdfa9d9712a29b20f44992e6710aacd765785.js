var gulp = require('gulp')
  , sass = require('gulp-sass')
  , gulpif = require('gulp-if')
  , del = require('del')
  , fs = require('fs')
  , argv = require('yargs').argv
  , download = require('gulp-download')
  , path = require('path')
  , runSequence = require('run-sequence')
  , sourcemaps = require('gulp-sourcemaps')
  , symlink = require('gulp-symlink')
  , uglify = require('gulp-uglify')
  , beautify = require('gulp-beautify')
  , pkg = require('./package.json')
  , rootpath = 'docroot/sites/all/themes/' + pkg.name
;

// Set external js files
var ext_js = [
  'http://code.jquery.com/jquery-latest.js',
  'https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js'
];

// Set external css files
var ext_css = [
  'https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css'
];

// Clean folders
gulp.task('clean', function() {
  return del(['theme/ui/css/**/*.css', rootpath]);
});

// Download external css
gulp.task('external-css', function(cb) {
  // ensure files don't exist before downloading them
  var urls = [];
  ext_css.forEach(function(url, index) {
    try {
      fs.lstatSync('theme/ui/lib/css/' + path.basename(url));
    }
    catch(e) {
      urls.push(url);
    }
  });

  if (!urls.length) {
    return cb();
  }

  return download(urls)
    .pipe(gulp.dest('theme/ui/lib/css'));
});

// Download external js
gulp.task('external-js', function(cb) {
  // ensure files don't exist before downloading them
  var urls = [];
  ext_js.forEach(function(url, index) {
    try {
      fs.lstatSync('theme/ui/lib/js/' + path.basename(url));
    }
    catch(e) {
      urls.push(url);
    }
  });

  if (!urls.length) {
    return cb();
  }

  return download(urls)
    .pipe(gulp.dest('theme/ui/lib/js'));
});

// Process js
gulp.task('scripts', function(cb) {
  var path = 'theme/ui/js/**/*.js';

  if (argv.develop) {
    return cb();
  }

  return gulp.src(path)
    .pipe(sourcemaps.init())
    .pipe(uglify({ preserveComments: "license" }))
    .pipe(sourcemaps.write())
    .pipe(gulp.dest('theme/ui/js'));
});

// Compile sass
gulp.task('styles', function() {
  var path = 'theme/ui/sass/**/*.scss';

  if (argv.develop) {
    gulp.watch(path, ['styles']);
  }

  return gulp.src(path)
    .pipe(gulpif(argv.develop, sass().on('error', sass.logError), sass({ outputStyle: "compressed" }).on('error', sass.logError)))
    .pipe(gulp.dest('theme/ui/css/'));
});

// Symlink theme files
gulp.task('symlink', function() {
  return gulp.src('theme')
    .pipe(symlink(rootpath));
});

// Move theme files
gulp.task('move', function() {
  return gulp.src(['theme/**/*.*', '!theme/ui/sass/**/*.scss'])
    .pipe(gulp.dest(rootpath));
});

// Determine whether theme files should be symlinked or moved
gulp.task('move-symlink', function(cb) {
  if (argv.develop) {
    runSequence('symlink', cb);
  } else {
    runSequence('move', cb);
  }
});

// Prepare Drupal settings for environment
gulp.task('settings', function(cb) {
  var filepath = 'docroot/sites/default/settings.php';

  try {
    fs.lstatsync(filepath);
  }
  catch (e) {
    // settings.php does not exist, let's create it
    var code = "$home = getenv('HOME');\n"
             + "if (!empty($home)) {\n"
             + "  $home = rtrim($home, '/');\n"
             + "}\n"
             + "\n"
             + "require_once $home . '/" + pkg.name + "-db.php';";
    fs.writeFileSync(filepath, fs.readFileSync('docroot/sites/default/default.settings.php') + "\n" + code);
  }

  if (argv.develop) {
    return gulp.src('settings.local.php')
      .pipe(symlink('docroot/sites/default/settings.local.php', { force: true }));
  }

  cb();
});

// Default task
gulp.task('default', function(cb) {
  runSequence('clean', 'styles', 'external-js', 'move-symlink', 'settings', cb);
});