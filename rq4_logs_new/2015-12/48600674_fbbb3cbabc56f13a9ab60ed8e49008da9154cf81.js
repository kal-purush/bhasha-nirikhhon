"use strict";

var fs = require('fs');
var process = require('process');
var dslcore = require('../lib/index');

let output = dslcore.compileFile('xszr.ds');
var grammarok = dslcore.checkGrammar(output, function (isok, err) {
    if (!isok) {
        console.log('Error => ' + err);
    }
});

if (!grammarok) {
    console.log('Error => checkGrammar()');

    process.exit(0);
}

output = dslcore.procInMessage(output);
output = dslcore.reverseObj(output);

console.log('dslang compile finished!');
fs.writeFileSync('xszr.json', JSON.stringify(output), 'utf-8');

dslcore.exportExcel('xszr', output, function (isok, err) {
    if (!isok) {
        console.log('Error => ' + err);
    }
});