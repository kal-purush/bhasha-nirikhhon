/// <reference path="../typings/node/node.d.ts" />

import { createInterface } from 'readline';
import { ask } from 'tslib'; // Should find tslib in node_modules
// let tslib = require('tslib');

const rl = createInterface({
  input: process.stdin,
  output: process.stdout,
});

rl.question('What would you ask? ', function(query) {
  let answer: number = ask(query); // Should fail checking; ask => string
  console.log(answer);
  process.exit();
});