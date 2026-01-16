#!/usr/bin/env node

var Find = require('find-file');
var replace = require('replace');

var args = process.argv.slice(2);

var name = args[0];
var path = args[1];
var regex = args[2];
var replacement = args[3];

var find = new Find()
    .name(name)
    .where([path]);

find.run(function (err, files) {

    if (err || !files.length) return;

    replace({
        regex: new RegExp(regex, 'g'),
        replacement: replacement,
        paths: [files[0].path]
    });

});