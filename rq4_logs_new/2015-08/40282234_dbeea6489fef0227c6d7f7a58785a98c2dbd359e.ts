/// <reference path="stdIn.ts" />

stdIn.ready(main);

function solveMeFirst(a, b) {
    return a+b;
}

function main() {
    // write your code here.
    // call `readLine()` to read a line.
    // use console.log() to write to stdout

    var a = parseInt(stdIn.readLine());
    var b = parseInt(stdIn.readLine());

    var res = solveMeFirst(a, b);
    console.log(res);

}