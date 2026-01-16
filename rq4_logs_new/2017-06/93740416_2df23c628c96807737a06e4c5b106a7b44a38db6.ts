"use strict";

import * as yaml from "js-yaml";
import * as fs from "fs";

// promisify fs operations
let fsRead = (file: string, encoding?: string) => new Promise<string>((resolve, reject) => {
  let callback = (err, data) => {
    if (!err) { resolve(data); }
    else { reject(err); };
  };
  if (encoding) { fs.readFile(file, encoding, callback); }
  else { fs.readFile(file, callback); }
});
let fsWrite = (file: string, content: any, options?: { encoding?: string; mode?: string; flag?: string; }) =>
  new Promise<void>((resolve, reject) => {
    let callback = (err) => {
      if (!err) { resolve(); }
      else { reject(err); };
    };
    if (options) { fs.writeFile(file, content, options, callback); }
    else { fs.writeFile(file, content, callback); }
  });

export = class Config {
  private filePath: string;
  public data = {};
  constructor(filePath: string) {
    this.filePath = filePath;
  }
  pull = async () => {
    try {
      this.data = yaml.safeLoad(await fsRead(this.filePath));
    } catch (err) {
      if (err.code === 'ENOENT') {
        await this.touch();
      } else {
        throw err;
      }
    }
  }
  pullWhenExists = async () => {
    this.data = yaml.safeLoad(await fsRead(this.filePath));
  }
  pullSync = () => {
    try {
      this.data = yaml.safeLoad(fs.readFileSync(this.filePath).toString());
    } catch (err) {
      if (err.code === 'ENOENT') {
        this.touchSync();
      } else {
        throw err;
      }
    }
  }
  pullSyncWhenExists = () => {
    this.data = yaml.safeLoad(fs.readFileSync(this.filePath).toString());
  }
  push = async () => {
    if (this.data === void 1) {
      this.data = {};
    }
    await fsWrite(this.filePath, yaml.safeDump(this.data));
  }
  pushSync = () => {
    fs.writeFileSync(this.filePath, yaml.safeDump(this.data));
  }
  touch = async () => {
    await this.push();
  }
  touchSync = () => {
    this.pushSync();
  }
  get = (name: string) => {
    if (this.data[name]) {
      return this.data[name];
    } else {
      throw new Error("invalid config key");
    }
  }
  set = (name: string, data: any) => {
    if (this.data) {
      this.data[name] = data;
    }
    else {
      this.data = {};
      this.set(name, data);
    }
  }
  setWhenExists = (name: string, data: any) => {
    if (name in this.data) {
      this.set(name, data);
    } else {
      throw new Error("config key not defined");
    }
  }
};