const csso = require('csso');
const path = require('path');
const fs = require('fs');

const cssOutputPath = path.resolve(__dirname, '..', 'dist', 'index.css');
const cssMinifiedOutputPath = path.resolve(__dirname, '..', 'twa.css');

const minified = csso.minify(fs.readFileSync(cssOutputPath, { encoding: 'utf-8' })).css;

fs.writeFileSync(cssMinifiedOutputPath, minified);