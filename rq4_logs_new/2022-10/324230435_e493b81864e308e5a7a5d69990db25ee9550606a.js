#!node
const doc = `
Goal: Make it easy and consistent to start a new typescript project
Tenet #1: Start with most basic
Tenet #2: Add functionality as desired
Tenet #3: Select from pre-canned, or create a new configuration

Usage: ${__filename} <path> [<config>]
 - <path>: where to create the project; final element of path will be project directory
 - <config>: special configuration to use; none currently supported

Run this script to create a new barebones typescript node project as described here:
https://www.typescripttutorial.net/typescript-tutorial/nodejs-typescript/

With this basic setup, your typescript will automatically be recompiled and run as Javascript
as you make changes.  Following the doc, we use npm and supporting modules, but should provide
other configurations as desired.

`;

const docopt = require('@eyalsh/docopt').default;

let options = docopt(doc);
let path = options['<path>'];
let config = options['<config>'];

// Writes usage, along with optional error
// @Returns 1 if error, else 0
function usage(msg) {
    if (msg) console.error(msg);
    console.log(doc);
    return msg ? 1 : 0;
}

if (!path) usage("<path> not specified");
if (!config) usage("<config> not specified");

// BASE setup

// create path/build and path/src
// change to path current directory
// run tsc --init; try with --rootDir ./src and --outDir ./build (check tsconfig.json)
// create file src/app.ts
// run 'tsc'
// run 'node ./build/app.js to verify working
// run 'npm init --yes' to set up the node directory
// npm install --g nodemon concurrently
// add these to "scripts" in package.json:
//          "start:build": "tsc -w",
//          "start:run": "nodemon build/app.js",
//          "start": "concurrently npm:start:*"
// change "main" to "app.js" in package.json
