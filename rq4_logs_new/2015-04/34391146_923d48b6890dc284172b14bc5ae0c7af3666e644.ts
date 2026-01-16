/// <reference path="../typings/all.d.ts" />
import path = require("path");
import fs = require("fs");
import _ = require("lodash");
import fse = require('fs-extra');
import fsf = require('fs-finder');

var nowPath = '.';
var definitelyTypedPath = path.resolve('./DefinitelyTyped/');

// read package.json
var packageJson = fse.readJsonSync(path.join(nowPath, 'package.json'));

var dependencies = _.keys(packageJson['dependencies']);
var devDependencies = _.keys(packageJson['devDependencies']);

var DotDDotTsFiles: string[] = [];

function copyFile(filename: string) {
  var from = path.join(definitelyTypedPath, filename);
  var to = path.join(nowPath, 'typings', filename);
  console.log(`from ${from} to ${to}`);
  var files = fsf.in(from).findFiles();
  DotDDotTsFiles = DotDDotTsFiles.concat(files);
  console.log(path.resolve(definitelyTypedPath));
  fse.copy(from, to, (err) => {
    if (err) {
      console.log(err);
    }
  });
}

var files = fs.readdirSync(definitelyTypedPath);
var o = _.object(files, files);

function run(modules: string[]) {
  for (var i in modules) {
    var t: string = modules[i];
    if (o[t]) {
      console.log(`hava ${t}`);
      copyFile(t);
    } else {
      console.log(`no ${t}`);
    }
  }
}

run(dependencies);
run(devDependencies);
run(['node']);

console.log('------------------');
console.log(DotDDotTsFiles);
DotDDotTsFiles = _(DotDDotTsFiles).chain().map((it) => it.slice(it.indexOf('DefinitelyTyped/') + 'DefinitelyTyped/'.length)).filter((it) => {
  var l = it.split('/');
  return l.length == 2 && l[0] + '.d.ts' == l[1];
}).value();

var allDOtDDOTTsContent = '';
for (var i in DotDDotTsFiles) {
  var t = DotDDotTsFiles[i];
  allDOtDDOTTsContent += `/// <reference path="./${t}" />\n`;
}
console.log(allDOtDDOTTsContent);
fse.writeFile(path.join(nowPath, 'typings', 'all.d.ts'), allDOtDDOTTsContent);