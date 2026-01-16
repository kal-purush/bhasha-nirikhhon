#!/usr/bin/env node

const resolve = require('resolve');

const rpPath = resolve.sync('rainypack-cli', { basedir: __dirname });
const args = process.argv.reduce((acc, cur) => {
  return [...acc, cur]
}, ['new'])

execa(rpPath, args, { stdio: 'inherit' }).catch((err) => {
  console.log('Error while executing rp new:', err.message);
  process.exit(1);
});