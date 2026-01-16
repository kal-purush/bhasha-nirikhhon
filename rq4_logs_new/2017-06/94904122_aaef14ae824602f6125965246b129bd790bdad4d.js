const file_system = require('fs');
const lodash = require('lodash');
const yargs = require('yargs');

const notes = require('./notes');

const yargs_argv = yargs.argv;
input_array = yargs_argv._;

let command = input_array[0];
let title = input_array[1];
let text = input_array[2];


if (command === "add") {
    console.log(notes.add(title, text));
}
else if (command === "list") {
    console.log(notes.list());
}
else if (command === "read") {
    console.log(notes.read(title));
}
else if (command === "remove") {
    console.log(notes.remove(title));
}
else if (command === "help") {
    console.log(notes.showHelp());
}
else {
    console.log(`command not recognized. Type help to see a list of commands`);
}