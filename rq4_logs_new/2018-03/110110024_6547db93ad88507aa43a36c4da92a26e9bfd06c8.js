// Copyright 2004-present Facebook.

const tsc = require('typescript');
const tsConfig = require('./tsconfig.json');

module.exports = {
  process(src, path) {
    if (path.endsWith('.ts') || path.endsWith('.tsx')) {
      return tsx.transpile(src, tsConfig.compilerOptions, path, []);
    }
    return src;
  }
}