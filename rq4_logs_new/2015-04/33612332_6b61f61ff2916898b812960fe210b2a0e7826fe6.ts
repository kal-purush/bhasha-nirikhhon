
import minimist = require("minimist");
import fs = require("fs");

function help() {
  var doc = `
md2xmd <file>

Converts a markdown file to extensible markdown format.
`
  console.log(doc);
  process.exit(0);
}

help();