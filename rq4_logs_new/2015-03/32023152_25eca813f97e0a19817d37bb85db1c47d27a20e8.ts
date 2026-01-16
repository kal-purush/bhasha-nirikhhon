/// <reference path="typings/gulp/gulp.d.ts" />
/// <reference path="typings/gulp-typescript/gulp-typescript.d.ts" />
/// <reference path="typings/gulp-inject/gulp-inject.d.ts" />
/// <reference path="typings/main-bower-files/main-bower-files.d.ts" />
/// <reference path="typings/browser-sync/browser-sync.d.ts" />
/// <reference path="typings/del/del.d.ts" />

import gulp = require('gulp')
import ts = require('gulp-typescript')
import del = require('del')
import mainBowerFiles = require('main-bower-files')
import inject = require('gulp-inject')
import browserSync = require('browser-sync')

var merge = require('merge2')

var tsProject = ts.createProject({
  declarationFiles: false,
  noExternalResolve: false
})

gulp.task('bs.init', ['inject'], () => {
  browserSync({
    server: {
      baseDir: ['./.tmp', './bower_components'], port: 4000
    }
  })
})

gulp.task('bs.reload', () => {
  browserSync.reload()
})

gulp.task('scripts', () => {
  var tsResult = gulp.src('src/**/*.ts').pipe(ts(tsProject))
  return merge([
    tsResult.js.pipe(gulp.dest('.tmp'))
  ])

})

gulp.task('inject', ['scripts'], () => {
  return gulp.src(['src/index.html'])
    .pipe(inject(gulp.src(mainBowerFiles().concat(['.tmp/**/*.js'])), {
      ignorePath: ['.tmp', 'bower_components']
    }))
    .pipe(gulp.dest('.tmp'))
})

gulp.task('serve', ['bs.init'], () => {
  gulp.watch('src/**/.ts', ['scripts'])
  gulp.watch(['.tmp/**/*.js'], ['bs.reload'])
})

gulp.task('clean', done => {
  del(['.tmp'], () => {
    done()
  })
})