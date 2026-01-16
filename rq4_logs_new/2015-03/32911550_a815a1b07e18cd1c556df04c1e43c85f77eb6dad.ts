/// <reference path="../types/fs-extra.d.ts" />
/// <reference path="../types/es6-promise.d.ts" />

import fs = require("fs-extra");
import modulePromise = require("es6-promise");

var Promise = modulePromise.Promise;

class FsPromise {

    public readJSON(path:string):Promise<any> {
        return new Promise((resolve, reject) => {
            fs.readJSON(path, (err, data) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

    public readFile(path:string, encoding:string):Promise<any> {
        return new Promise((resolve, reject) => {
            fs.readFile(path, encoding || "utf8", function (err, data) {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

}