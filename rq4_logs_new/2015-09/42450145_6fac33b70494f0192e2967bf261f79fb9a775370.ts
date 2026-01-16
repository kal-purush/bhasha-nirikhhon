///<reference path="../typings/vinyl/vinyl.d.ts" />
///<reference path="../typings/es6-promise/es6-promise.d.ts" />

import File = require('vinyl');

import {Transform} from "stream";

interface FilesTransformer {
  // This is the API we would have liked Gulp to have for us
  handle: (file: File) => Promise<File[]>;
  finish?: () => Promise<File[]>;
};

class GulpStreamWrapper extends Transform {
  private handler: FilesTransformer;

  constructor(handler: FilesTransformer) {
    super({objectMode:true});
    this.handler = handler;
  }
  
  
  _transform(chunk: string|Buffer, encoding: string, done: Function) {
    let file = <File>(<any>chunk);
    let that = this;
    let processResults = (files: File[]) => {
      files.map((file: File) => {
        that.push(file);
      });
    }
    
    if (chunk === null) {
      if (this.handler.finish) {
        this.handler.finish().then(processResults).then(() => {
          this.push(null);
          done();
        }).catch((reason:any) => {console.log(reason);done();});
      } else {
        done();
      }
    } else {
      this.handler.handle(file).then(processResults).then(() => {
        debugger
        done();
      }).catch((reason:any) => {console.log(reason);done();});;
    }
  }

  _flush(done:Function) {

    let that = this;
    let processResults = (files: File[]) => {
      files.map((file: File) => {
        that.push(file);
      });
    }

    if (this.handler.finish) {
      this.handler.finish().then((files:File[]) => {
        this.handler.finish().then(processResults).then(() => {
          this.push(null);
          done();
        });
      }).catch((reason:any) => {console.log(reason);done();});
    } else {
      done();
    }
  }
};

let createGulpStreamWrapper = (handler: FilesTransformer): Transform => {
  return new GulpStreamWrapper(handler);
}

export {FilesTransformer, createGulpStreamWrapper}