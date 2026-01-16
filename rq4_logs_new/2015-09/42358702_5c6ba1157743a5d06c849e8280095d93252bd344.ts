/// <reference path="../node/node.d.ts"/>
/// <reference path="../gulp/gulp.d.ts"/>

declare module "gulp-livereload" {
  
  import stream = require('stream');
  import gulp = require('gulp');
  
  interface Options {
    port: number,
    host: string,
    basePath: string,
    quiet: boolean,
    reloadPage: string
  }
  
  function livereload(options?: Options): NodeJS.ReadWriteStream;
  
  module livereload {
    function listen(options?: Options): void;
    function changed(path: string): void;
    function reload(file?: string): void;
    function listen(): void;
    function listen(): void;
    var middleware: any; // tiny-lr.middletier
    var server: any; // tiny-lr
  }
  
  export = livereload;
}