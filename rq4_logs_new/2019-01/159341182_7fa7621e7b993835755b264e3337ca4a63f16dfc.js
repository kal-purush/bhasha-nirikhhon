/*
============================================
; Title:  Objects and Built-in Objects
; Author: Professor Krasso
; Date:   17 January 2019
; Modified by: Andrew Hemminger
; Description: Exercise 6.1 Objects and Built-in Objects
;===========================================
*/
// var header = require('../hemminger-header.js');
// console.log(header.display('Andrew', 'Hemminger', 'Exercise 6.1'));
// console.log('\n');

/*
Expected Output:

properties of a = 1,1
properties of b = 2,undefined

*/

//Start program

var a = new Object({x:1, y:1, z:3});
var b = {};

Object.defineProperty(b, 'x', {
    value: 2,
    writable: true,
    configurable: true
});
delete a.z;
delete a.z;

console.log('properties of a = ' + a.x + ',' + a.y);
console.log('properties of b = ' + b.x + ',' + b.y);

//End program