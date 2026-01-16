/// <reference path="../../typings/node/node" />
import * as fs from 'fs';
import { spawn } from 'child_process';

const filename = process.argv[2];

fs.watch(filename, () => {
  let 
    ls = spawn('ls', ['lh', filename]),
	output = '';
	
  ls.stdout.on('data', (chunk) => {
     output += chunk.toString();	  
  });	
  ls.stdout.on('close', (chunk) => {
    let parts = output.split(/\s+/);
	console.dir([parts[0], parts[4], parts[8]]);	  
  });
});
console.log(`Now watching ${filename} for changes`);