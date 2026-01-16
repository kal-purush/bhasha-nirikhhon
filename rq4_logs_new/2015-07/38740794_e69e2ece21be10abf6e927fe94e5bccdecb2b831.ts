import * as through2 from 'through2'
import File = require('vinyl')
import {Config} from '../config'
import * as path from 'path'
import * as browserify from 'browserify'
import source = require('vinyl-source-stream')
import sourcemaps = require('gulp-sourcemaps')
import * as fs from 'fs'
import * as gutil from 'gulp-util'
import merge = require('merge-stream')

var watchify = require('watchify')
var buffer = require('vinyl-buffer')

export interface ClientOptions {
  www?: NodeJS.ReadWriteStream
  watch?: boolean
}

export function buildClient(options?:ClientOptions): NodeJS.ReadWriteStream {
  //options = options || {}
  let opts = {
    cache: {},
    packageCache: {},
    debug: true
  }
  return through2.obj(function(file: File, enc: string, callback: (error?, chunk?) => void) {
    let self = this
    let b = browserify(opts).add(file.path)
    function bundle() {
      return b.bundle()
        .pipe(source(file.path, file.base))
        .pipe(buffer())
        .pipe(sourcemaps.init({ loadMaps: true }))
        .pipe(sourcemaps.write('./'))
    }

    if (options.watch) {
      b = watchify(b)
      b.on('update', bundle)
    }
    b.on('log', gutil.log)

    let all:NodeJS.ReadWriteStream;
    if (options.www) {
      all = merge(options.www, bundle())
    } else {
      all = bundle()
    }
    all.pipe(through2.obj(function(file: File, enc: string, _callback: (error?, chunk?) => void) {
      self.push(file)
      _callback()
    }, function(){
      callback()
    }))
  })
}