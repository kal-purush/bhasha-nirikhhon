import checkRequiredFiles from './utils/checkRequiredFiles';
// import detect from 'detect-port';
// import chalk from 'chalk';
// import prompt from 'react-dev-utils/prompt.js';
import clearConsole from './utils/clearConsole';
// import runDevServer from './server';
import paths from '../config/paths-ts';

// const DEFAULT_PORT = process.env.PORT || 3000;

// warn and crash if required files are missing
if (!checkRequiredFiles([paths.indexHtml, paths.indexTs])) {
  process.exit(1);
}

console.log('========= start ===========');
// console.log('checkRequiredFiles', checkRequiredFiles);
// console.log('paths', paths);
// clearConsole();
console.log('========= end ===========');