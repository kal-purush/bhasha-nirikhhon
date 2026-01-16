/// <reference path='typings/node/node.d.ts' />

var bower = require('bower');
var Builder = require('systemjs-builder');
var connect = require('gulp-connect');
var del = require('del');
var exec = require('child_process').exec;
var gulp = require('gulp');
var plumber = require('gulp-plumber');
var rename = require('gulp-rename');
var sourcemaps = require('gulp-sourcemaps');
var typescript = require('gulp-tsc');
var watch = require('gulp-watch');

var paths = {
                lib : [
                      ],

                src : {
                    root: 'src',
                    files: [
                               'src/**/*.{yml,pem}',
                               'src/**/*.strings',
                               'src/**/*.{css,woff}',
                               'src/**/*.html',
                               'src/**/*.hbs',
                               'src/**/*.{png,gif,jpeg,jpg,ico}',
                           ],
                    js: 'src/**/*.js',
                    ts: 'src/**/*.ts'
                }
            };

gulp.task('clean', 
          (done) => {
              del(['dist'], done);
          });

gulp.task('bower', 
          (done) => {
              bower.commands
                   .install(null, 
                            { save: true }, 
                            { interactive: false }
                   )
                   .on('error', 
                       console.error.bind(console))
                   .on('end', () => {
                       done();
                   });
          });

gulp.task('libs', 
          ['bower'], 
          () => {
              return gulp.src(paths.lib)
                         .pipe(require('gulp-size')({ showFiles: true,
                                                      gzip: true }))
                         .pipe(gulp.dest('dist/lib'));
          });

gulp.task('ts', 
          () => {
              return gulp.src(paths.src.ts)
                         .pipe(rename({ extname: '' })) // hack, see: https://github.com/sindresorhus/gulp-traceur/issues/54
                         .pipe(plumber())
                         .pipe(sourcemaps.init())
                         .pipe(typescript({ module: 'commonjs',
                                            sourceMap: true,
                                            target: 'ES5' }))
                         .pipe(sourcemaps.write('.', 
                                                { sourceRoot: paths.src.root }))
                         .pipe(gulp.dest('dist'));
          });

gulp.task('js',
          () => {
              return gulp.src(paths.src.js)
                         .pipe(gulp.dest('dist'));
          });

gulp.task('files', 
          () => {
              return gulp.src(paths.src.files)
                         .pipe(gulp.dest('dist'));
          });

gulp.task('watch',
          [],
          () => {
              watch(paths.src.ts,
                    () => {
                      gulp.start('ts');
                    });

              watch(paths.src.js,
                    () => {
                      gulp.start('js');
                    });

              watch(paths.src.files,
                    () => {
                      gulp.start('files');
                    });
          });

gulp.task('play', 
          ['default', 'watch'],
          (cb) => {
              exec('node dist/bin/www.js',
                   (err, stdout, stderr) => {
                       console.log(stdout);
                       console.log(stderr);
                       cb(err);
                   })
          });

gulp.task('debug',
          ['default', 'watch'],
          (cb) => {
              exec('node --debug=45892 dist/bin/www.js',
                   (err, stdout, stderr) => {
                       console.log(stdout);
                       console.log(stderr);
                       cb(err);
                   })
          });

gulp.task('default', 
          [ 'libs', 'js', 'ts', 'files' ]);