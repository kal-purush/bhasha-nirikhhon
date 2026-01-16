import { resolve, dirname } from 'path';
import { File } from 'gulp-util';
import { existsSync, writeFileSync, mkdirSync, readFileSync, statSync } from 'fs';
import * as crypto from 'crypto';

export class CacheStore {
  private dir: string;
  constructor(key: string) {
    this.dir = resolve(process.cwd(), '.cache', key || 'temp');
    mkdirsSync(this.dir);
  }
  getCache(file: File): Cache | undefined {
    const cacheDir = this.getFolder(file);
    if (!existsSync(cacheDir + '.cache')) {
      return;
    }
    if (existsSync(cacheDir + '.deps')) {
      let diff = false;
      readFileSync(cacheDir + '.deps').toString().split('\r').forEach(line => {
        const arr = line.split('\t');
        if (diff === false && arr.length === 2) {
          if (existsSync(arr[0]) && statSync(arr[0]).mtimeMs.toString() === arr[1]) {
            // 符合
          } else {
            diff = true;
          }
        }
      });
      if (diff) {
        return;
      }
    }
    return {
      contents: readFileSync(cacheDir + '.cache')
    }
  }

  saveCache(file: File) {
    if (file && file.stat && file.stat.mtimeMs) {
      const cacheDir = this.getFolder(file);
      if (file.depFiles) {
        let error = false;
        let depinfo = '';
        file.depFiles.forEach(dep => {
          if (existsSync(dep)) {
            const stat = statSync(dep);
            depinfo +=`${dep}\t${stat.mtimeMs}\r`;
          } else {
            error = true;
          }
        });
        if (error) {
          return;
        }
        writeFileSync(cacheDir + '.deps', depinfo);
      }
      writeFileSync(cacheDir + '.cache', file.contents);
      writeFileSync(cacheDir + '.info', `${file.path}\r${file.stat.mtimeMs}\r`);
    }
  }

  getFolder(file: File) {
    let md5sum = crypto.createHash('md5');
    md5sum.update(file.path + file.stat.mtimeMs, 'utf8');
    return this.dir + '/' + md5sum.digest('hex').substring(0,32);
  }

  proxy(t: transform) {
    if (process.env.build_cache === "open") {
      return (file: File, enc, callback: Callback) => {
        const cache = this.getCache(file);
        if (cache) {
          file.contents = cache.contents;
          callback(null, file);
        } else {
          t(file, enc, (buffer, file) => {
            this.saveCache(file);
            callback(null, file);
          });
        }
      }
    } else {
      return t;
    }
  }
}

type transform = (file: File, enc, callback) => void;
type Callback =  (buffer: any, file: File) => void;
interface Cache {
  contents: Buffer;
}

import stream = require('readable-stream');

export class Transform extends stream.Transform {
  constructor(option :any) {
    const cache = new CacheStore('amdWrap');
    if (option.transform) {
      option.transform =  cache.proxy(option.transform);
    }
    super(option);
  }
}


function mkdirsSync(dir) {
  if (existsSync(dir)) {
    return true;
  } else {
    if (mkdirsSync(dirname(dir))) {
      mkdirSync(dir);
      return true;
    }
  }
}