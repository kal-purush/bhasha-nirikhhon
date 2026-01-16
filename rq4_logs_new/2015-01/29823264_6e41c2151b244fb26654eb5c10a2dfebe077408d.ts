///<reference path="declarations/node.d.ts"/>
import fs = require("fs");
import path = require("path");

"use strict";

var args: string[] = process.argv.slice(2);
if(args.length !== 2) {
    usage();
    return;
}

var dirPath: string = args[0];
var mergeName: string = args[1];
var dirList: string[] = fs.readdirSync(dirPath);
var data: { [key: string]: any[] } = {};
data[mergeName] = [];

dirList.forEach(function (val) {
    var fullPath = path.join(dirPath, val);
    // converts bytes into string format with "" concatenation
    var mergeObj: any = JSON.parse(fs.readFileSync(fullPath)+"");
    data[mergeName].push(mergeObj);
});

fs.writeFileSync(mergeName + ".json", JSON.stringify(data));

/* Prints a usage statement to the user */
function usage() {
    console.log("Usage: node JSONMerger.js [dir] [mergeName]");
}

console.log("--tyca :)");