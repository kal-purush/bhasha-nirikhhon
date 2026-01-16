/*global require: true*/

'use strict';

var gulp = require('gulp');
var gutil = require('gulp-util');
var rename = require('gulp-rename');
var svgstore = require('gulp-svgstore');
var svgmin = require('gulp-svgmin');
var svgfallback = require('gulp-svgfallback');
var cheerio = require('gulp-cheerio');
var inject = require('gulp-inject');

var svgSourceFilesGlob = 'public/img/icons/src/*.svg';

gulp.task('icons:generate-svg-source', function () {

  var svgSourcePartialTemplate = 'public/img/icons/source-template.hbs';
  var svgSourcePartialDestination = 'public/templates/partials/icons';

  var svgContents = function svgContents (path, svg) {
    return svg.contents.toString();
  };

  var svgs = gulp.
    src(svgSourceFilesGlob, {base: 'src/icons'})
    .pipe(svgmin(function () {
      return { plugins: [{ cleanupIDs: { minify: true }}] };
    }))
    .on('end', function doneMinifying () {
      gutil.log('Minifying SVG source');
    })
    .pipe(cheerio({
      run: function ($) {
        $('[fill]').removeAttr('fill');
      },
      parserOptions: { xmlMode: true }
    }))
    .on('end', function doneStrippingFillColor () {
        gutil.log('Removing fill attributes');
    })
    .pipe(rename({prefix: 'icon-'}))
    .on('end', function doneRenamingIds () {
      gutil.log('Prefixed symbol ids with icon-');
    })
    .pipe(svgstore({ inlineSvg: true }));

  return gulp
    .src(svgSourcePartialTemplate)
    .pipe(inject(svgs, { transform: svgContents }))
    .pipe(rename('iconSource.hbs'))
    .on('end', function doneRenamingSvgSourceTemplate () {
      gutil.log('Renamed source-template.hbs to iconSource.hbs');
    })
    .pipe(gulp.dest(svgSourcePartialDestination))
    .on('end', function doneWritingIconSource () {
      gutil.log('iconSource.hbs saved to ' + svgSourcePartialDestination);
    });
});

gulp.task('icons:generate-svg-fallback', function () {
  return gulp
    .src(svgSourceFilesGlob, {base: 'src/icons'})
    .pipe(rename({prefix: 'icon-'}))
    .pipe(svgfallback({
      cssTemplate: 'public/img/icons/pngFallbackStyle-template.css',
      backgroundUrl: '/img/icons/dist/icons-fallback.png'
    }))
    .on('end', function () {
      gutil.log('Generated PNG from SVG source');
    })
    .pipe(rename(function distributeFallbackAssets (path) {
      if ('.css' === path.extname) {
        path.dirname = 'public/css';
        path.basename = 'icons-fallback';
      }
      if ('.png' === path.extname) {
        path.dirname = 'public/img/icons/dist';
        path.basename = 'icons-fallback';
      }
    }))
    .on('end', function doneMovingFallbackAssets () {
      gutil.log('Moving icons-fallback.less to public/css');
      gutil.log('Moving icons-fallback.png to public/img/icons/dist');
    })
    .pipe(gulp.dest('./'));
});

gulp.task('icons', [
  'icons:generate-svg-source',
  'icons:generate-svg-fallback'
]);