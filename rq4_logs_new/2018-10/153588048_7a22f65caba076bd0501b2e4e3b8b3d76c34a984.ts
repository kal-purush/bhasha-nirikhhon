import fs from 'fs';
import path from 'path';
import rimraf from 'rimraf';
import { appendTestResults, compress, decompress } from './appendTestResults';
import { setup } from './setup';
import { coverageWithPercentageMinified, noCoverage } from './testResultsExamples';


test('create new file', () => {
  setup({ PROGRESS_FOLDER: '.new_file' })
  appendTestResults({ fs }, path.join('.new_file', 'newfile'), noCoverage)
  rimraf.sync('.new_file')
})

test.only('asdf', () => {
  const actual = decompress(compress(coverageWithPercentageMinified))

  console.log(actual)
})