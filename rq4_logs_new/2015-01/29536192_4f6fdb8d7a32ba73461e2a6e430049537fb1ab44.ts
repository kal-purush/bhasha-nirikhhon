/**
 * Created by barais on 20/01/15.
 */
///<reference path='collections/collections.d.ts' />
///<reference path='node/node.d.ts' />
///<reference path='mraa.d.ts' />

import fs = require("fs")
import mraa= require("mraa")

var col = fs.readFileSync('./collections/collections.js','utf8');
eval(col);

console.log(mraa.getVersion())

var myLed = new mraa.Gpio(13); //LED hooked up to digital pin 13 (or built in pin on Galileo Gen1 & Gen2)
myLed.dir(1); //set the gpio direction to output
var ledState = true; //Boolean to hold the state of Led

setInterval(function(){
    myLed.write(ledState?1:0); //if ledState is true then write a '1' (high) otherwise write a '0' (low)
    ledState = !ledState; //invert the ledState
},1000);