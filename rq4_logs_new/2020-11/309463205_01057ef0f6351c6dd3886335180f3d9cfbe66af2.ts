/* eslint-disable */
const fs = require('fs');
const path = require('path');

fs.readFile(path.resolve(__dirname, '../tsconfig.json'), 'utf-8', (err, data) => {
  if (err) {
    console.error(err);
    return;
  }
  const result = data.replace('"isolatedModules": false', '"isolatedModules": true');
  fs.writeFile(path.resolve(__dirname, '../tsconfig.json'), result, 'utf8', (err) => {
    if (err) return console.log(err);
    console.log('Changed flag to false for isloated modules.');
  });
});