import path from 'path';
import { fileURLToPath } from 'url';
import genDiff from '../src/index.js';
// import getPath from '../src/getPath.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const getFixturePath = (filename) => path.join(__dirname, '..', '__fixtures__', filename);

const first = getFixturePath('file1.json');
const second = getFixturePath('file2.json');

const expected = `{
- follow: false
  host: hexlet.io
- proxy: 123.234.53.22
- timeout: 50
+ timeout: 20
+ verbose: true
}`;

test('difference test 1', () => {
  expect(genDiff(first, second)).toEqual(expected);
});