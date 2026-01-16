import * as mongoose from "mongoose";
import * as Promise from "bluebird";
import config from "./config";

module Mongoose {
  
  export function connect () {
    return new Promise (function (resolve, reject) {
      
      var db = mongoose.connect(config.db.uri, config.db.options, function (err) {
        if (err) {
          console.log(err);
          reject(err);
        } else {
          mongoose.set('debug', config.db.debug);

          resolve(db);
        }
      });
      
    });
  };
  
  export function disconnect () {
    return new Promise (function (resolve, reject) {
      mongoose.disconnect(function (err) {
        if (err) reject(err);
        else resolve({});
      });  
    });
  };
  
}

export {Mongoose as default};