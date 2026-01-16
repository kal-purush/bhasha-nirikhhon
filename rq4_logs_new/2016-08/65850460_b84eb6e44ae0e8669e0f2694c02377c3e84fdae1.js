//  TEST FILE FOR JASMINE (can be deleted)

'use strict'

/*const program = require('commander'),
    exec = require('child_process').exec,
    pkg = require('./package.json');

//console.log('DOES THIS WORK?');
exec('thingshub', (err, stdout, stderr) => {
  if (err) {
    console.error(err);
    return;
  }
  console.log(stdout);
});
*/

/*const exec = require('child_process').exec;
const bat = spawn('node ./thingshub', ['-l']);

bat.stdout.on('data', (data) => {
  console.log(data);
});

bat.stderr.on('data', (data) => {
  console.log(data);
});

bat.on('exit', (code) => {
  console.log(`Child exited with code ${code}`);
});*/

// OR...
const 	exec = require('child_process').exec;

exec('node ./thingshub -l', (err, stdout, stderr) => {
  if (err) {
    console.error(err);
    return;
  }
  console.log(stderr);
  console.log(stdout);
});