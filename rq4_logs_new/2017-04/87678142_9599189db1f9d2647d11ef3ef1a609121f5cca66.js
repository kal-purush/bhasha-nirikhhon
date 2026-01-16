"use strict";

var BasicCard = require("./basicFlash");
//var ClozeFlash = require("./ClozeFlash");

var Q = process.argv[2];
var I = process.argv[3];


var myQ = BasicCard

if (Q === undefined) {
    console.log("Sorry, you must enter Basic or Cloze");
    return
}

else if (Q.toLowerCase() === "basic") {
    console.log("you've selected basic")
    if ( I.toLowerCase() === "front") {
       console.log(myQ.front);
       
    }
    //var Basic = new Basic(question, answer);

}

else if (Q.toLowerCase() === "cloze") {
    console.log("you've selected cloze")
}