/// <reference path="../typings/node/node.d.ts" />

var packageJson = require("./../package.json");
console.log("Extropy-CLI");
console.log("Version: " + packageJson.version);

var argv = require("yargs")
    .usage("Usage: $0 <command> [options]")
    .command("init", "Create a new game or component")
    .command("publish", "Publish a game or component")
    .command("query", "Query the store for games or components")
    .command("play", "Play game")
    .help("h")
    .alias("h", "help")
    .epilog("Copyright 2015")
    .argv;

console.log("extropy: " + argv.$0);