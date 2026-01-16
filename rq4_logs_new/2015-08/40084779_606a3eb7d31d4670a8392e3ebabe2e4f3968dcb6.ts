/// <reference path="../../typings/node/node.d.ts" />
var findParentDir = require("find-parent-dir");
var path = require("path");
var extropy = require("../host/extropy-host.js")

var packageJson = require("./../../package.json");
console.log("Extropy-CLI");
console.log("Version: " + packageJson.version);

var activeExpressServer = null;

var argv = require("yargs")
    .usage("Usage: $0 <command> [options]")
    .command("init", "Create a new game or component")
    .command("publish", "Publish a game or component")
    .command("query", "Query the store for games or components")
    .command("play", "Pl[bay game", play)
    .help("h")
    .alias("h", "help")
    .epilog("Copyright 2015")
    .argv;

function play(yargs): void {
    console.log("PLAY3");

    try {
        var dir = findParentDir.sync(process.cwd(), "game.json");
        console.log(dir);

        if(!dir){
            console.log("No game.json found.")
        } else {
            startGame(dir, path.join(dir, "game.json"));
        }
    } catch(err) {
        console.log("Error: " + err);
    }
}

function startGame(workingDirectory: string, pathToGameJson: string[]) {
    console.log("Trying to start game.");
    var hostServer = new extropy.GameHostServer();

}