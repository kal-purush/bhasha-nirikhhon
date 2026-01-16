'use strict';

const PolymerProject = require('polymer-build').PolymerProject;
const gulp = require('gulp');
const mergeStream = require('merge-stream');
const forkStream = require('polymer-build').forkStream;

const project = new PolymerProject(require('./polymer.json'));

gulp.task('build-unbundled', () => {
  return mergeStream(project.sources(), project.dependencies())
    .pipe(gulp.dest('build/unbundled'));
})

gulp.task('build-bundled', () => {
  return mergeStream(project.sources(), project.dependencies())
    .pipe(project.bundler)
    .pipe(gulp.dest('build/bundled'));
})


