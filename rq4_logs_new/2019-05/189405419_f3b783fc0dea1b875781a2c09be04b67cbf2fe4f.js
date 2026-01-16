const fs = require('fs-extra');
const concat = require('concat');

(async function build() {
  const files = [
    './dist/app-a/runtime-es2015.js',
    './dist/app-a/polyfills-es2015.js',
    './dist/app-a/scripts.js',
    './dist/app-a/main-es2015.js'
  ];

  await fs.ensureDir('elements');
  await concat(files, 'elements/app-a.js');
  await fs.copyFile(
    './dist/app-a/styles.css',
    'elements/styles.css'
  );
})();