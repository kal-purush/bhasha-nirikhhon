const execa = require('execa');
const move = require('move-concurrently');
const path = require('path');

const dist = 'dist';
const prefix = 'firefox-';

Promise.resolve().then(() => {
  return execa('npx', ['web-ext', 'build', '-s', 'dist-firefox', '-a', dist, '-o']);
}).then(result => {
  // value.stdout
  // blah blah blah\n
  // Your web extension is ready: dist/build_link_plain-2.0.0.zip
  const distRegex = /ready:\s(.*)/;
  const distFullName = result.stdout.match(distRegex)[1];
  const arr = distFullName.split('/');
  const distName = arr[arr.length - 1];
  const distPathArr = arr.slice(0, -1);
  const prefixedFullName = path.join(...distPathArr, `${prefix}${distName}`);
  return move(distFullName, prefixedFullName);
}).catch(error => {
  console.error(error); // eslint-disable-line no-console
  process.exit(1);
});