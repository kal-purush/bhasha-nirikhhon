'use strict';

var gulp = require('gulp'),
  $ = require('gulp-load-plugins')(),
  jsonSorter = require('ute-json-sorter'),
  tagConverter = require('ute-tag-converter');

/**
 * Properly sorts JSON alphabetically
 */
gulp.task('sort-json', function () {

  return gulp.src(['lang/**/*.json'])
    .pipe(jsonSorter())
    .pipe(gulp.dest('lang-output'));

});

/**
 * Converts CSV into one json object
 */
gulp.task('convert-adobe-tags-test', function () {

  return gulp.src(['output/csv/test-click-1.csv', 'output/csv/test-pageload-2.csv'])
    .pipe(tagConverter('tags.json'))
    .pipe(gulp.dest('output/csv-output'));

});

/**
 * Converts CSV into one json object
 */
gulp.task('convert-adobe-tags', function () {

  return gulp.src(['output/csv/on-click.csv', 'output/csv/on-load.csv'])
    .pipe(tagConverter('ute-adobe-tags.json'))
    .pipe(gulp.dest('output/csv-output'));

});