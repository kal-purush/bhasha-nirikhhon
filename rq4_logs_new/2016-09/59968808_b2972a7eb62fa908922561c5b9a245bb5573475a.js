#!/usr/bin/env node
'use strict';

// TODO: confirm package data and location
const pkg = require('../package.json');

// standard libraries
const fs   = require('fs');
const path = require('path');

// TODO: npm install --save commander
const program = require('commander');

// TODO: chmod +x
// TODO: npm link

program
  .version(pkg.version)
  .description(pkg.description)
  .option('-o, --output [type]', 'Add the specified type of output [ndjson|json|js]',  'ndjson')
  .usage('[options] <SQL')
  .parse(process.argv);

let query = '';
process.stdin.on('readable', function () {
  const chunk = process.stdin.read();
  if (chunk !== null)
    query += chunk;
});

process.stdin.on('end', function () {

});