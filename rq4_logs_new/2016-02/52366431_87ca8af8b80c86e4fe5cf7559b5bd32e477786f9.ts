/// <reference path="../typings/main.d.ts" />
import npm = require('npm');
import Sln from '../lib/sln';
const path = require('path');

const config = {
    folder: 'packages'
};
const options = require('command-line-args')([
    { name: 'command', type: String, multiple: true, defaultOption: true }
]).parse();
const action = options.command.pop();
const packageName = options.command.pop();

console.info('Running', action, 'on', packageName, '...');

var sln = new Sln(packageName);
sln.run(action)
    .then(() => {
        console.info('Run', action, 'successfully');
    })
    .catch((err) => {
        console.error('Failed to run', action);
        process.exit(1);
    })