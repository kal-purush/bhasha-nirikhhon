#! /usr/bin/env node

/// <reference path="../typings/main.d.ts" />
import npm = require('npm');
import Sln = require('../lib/sln');
const path = require('path');

const config = {
    folder: 'packages'
};
const options = require('command-line-args')([
    { name: 'package', alias: 'p', type: String, multiple: true, defaultOption: true },
    { name: 'action', alias: 'a', type: String }
]).parse();

console.info('Running on', options.package, '...');

var sln = new Sln(options.package);
sln.run(options.action)
    .then(() => {
        console.info('Run',options.action,'successfully');
    })
    .catch((err) => {
        console.error('Failed to run',options.action);
        process.exit(1);
    })