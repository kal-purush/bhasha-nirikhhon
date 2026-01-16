const readFile = require('../helper.js');

const inputMatrix = readFile()
  .split('\n')
  .map((value) => value.split(''));