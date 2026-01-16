// LICENSE : MIT
"use strict";
function db(operation:string, key:string, value?:any):any {
    if (!db.data) {
        db.data = {};
    }
    var data = db.data;
    var oldValue:any;
    if (operation === "update") {
        oldValue = data[key];
        data[key] = value;
        return oldValue;
    } else if (operation === "read") {
        return data[key]
    }
}

module db {
    export var data:any;

    export function update(key, value) {
        return db("update", key, value);
    }

    export function read(key) {
        return db("read", key);
    }
}

var key = "str";
db.update(key, "valuE");
console.log(db.read(key));


db("update", "keyaaaa", "文字");
console.log(db("read", "keyaaaa"));