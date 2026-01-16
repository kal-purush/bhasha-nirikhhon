var gulp = require('gulp');
var gutil = require('gulp-util');
var concat = require('gulp-concat');
var jade = require('gulp-jade');
var uglify = require('gulp-uglify');
var sourcemaps = require('gulp-sourcemaps');
var minifyCss = require('gulp-minify-css');
var imagemin = require('gulp-imagemin');
var jshint = require('gulp-jshint');


gulp.task('default', ['copy-images','jshint', 'copy-libraries', 'build-css', 'jade-partials', 'build-app'], function(){
    gutil.log('Gulp ran');
});

// Copies and minifies images from server to client
gulp.task('copy-images', function() {
    return gulp.src('./server/app/assets/img/**.*')
        .pipe(imagemin({
            progressive: true
        }))
        .pipe(gulp.dest('./client/app/assets/img'))
});

gulp.task('jshint', function() {
    return gulp.src(['./server/app/app.js',
            './server/app/factories/**/*.js',
            './server/app/services/**/*.js',
            './server/app/views/partials/**/*.js',
            './server/app/views/partials/dialogs/**/*.js'])
        .pipe(jshint())
        .pipe(jshint.reporter('jshint-stylish'));
});


// Copies library scripts from node_modules to client
gulp.task('copy-libraries', function() {
    return gulp.src([
        'bower_components/pdfmake/build/pdfmake.min.js',
        'bower_components/pdfmake/build/vfs_fonts.js',
        'node_modules/underscore/underscore-min.js',
        'node_modules/moment/min/moment.min.js',
        'node_modules/angular/angular.min.js',
        'node_modules/angular-animate/angular-animate.min.js',
        'node_modules/angular-aria/angular-aria.min.js',
        'node_modules/angular-material/angular-material.min.js',
        'node_modules/angular-material/angular-material.min.css',
        'node_modules/angular-route/angular-route.min.js',
        'node_modules/angular-messages/angular-messages.min.js',
        'node_modules/angular-ui-grid/ui-grid.min.js',
        'node_modules/angular-ui-grid/ui-grid.woff',
        'node_modules/angular-ui-grid/ui-grid.ttf',
        'node_modules/angular-material-data-table/dist/md-data-table.min.js',
        'node_modules/angular-material-data-table/dist/md-data-table.min.css',
        'node_modules/ng-file-upload/dist/ng-file-upload.min.js',
        'node_modules/angular-resource/angular-resource.min.js'])
        .pipe(gulp.dest('client/app/assets/vendor'));
});

// Copies and uglifies app.js and all associated controllers from server to client
gulp.task('build-app', function() {
    return gulp.src(['./server/app/app.js',
        './server/app/factories/**/*.js',
        './server/app/services/**/*.js',
        './server/app/views/partials/**/*.js',
        './server/app/views/partials/dialogs/**/*.js'])
        .pipe(sourcemaps.init())
        .pipe(concat('app.min.js'))
        .pipe(uglify())
        .pipe(sourcemaps.write())
        .pipe(gulp.dest('./client/app/assets/js'))
});

// Copies and minifies all custom css from server to client
gulp.task('build-css', function() {
    return gulp.src('./server/app/assets/css/**.css')
        .pipe(sourcemaps.init())
        .pipe(concat('styles.min.css'))
        .pipe(minifyCss())
        .pipe(sourcemaps.write())
        .pipe(gulp.dest('./client/app/assets/css'))
});

// Compiles into HTML all jade-partials file from client to server
gulp.task('jade-partials', function() {
   return gulp.src('./server/app/views/partials/**/*.jade')
       .pipe(jade())
       .pipe(gulp.dest('./client/app/views/partials'))
});