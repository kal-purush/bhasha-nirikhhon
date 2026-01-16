"use strict";
//let is the new var
let x = 5; //implicit x is a number
let y:number = 5;//explicit y is a number
const MY_NUMBER = 10;

let z:string = "hello";
let me: boolean = true;
let things: string [] = [];
let other_things: number[]=[];
let many_things: Array<any>[] = ["hello",5,5];
let you: Object = {};
let multiple:any="test";
multiple = 4;

let result = addNumbers(5, 7);

function stuff():void{
    console.log(x);
    return;//returns undefined
}
/**
 * This function takes two numbers and adds them together.
 * @param {number} x Number 1
 * @param {number} y Number 2
 * @return {number} The sum of Number 1 + Number 2
 */

function addNumbers(x: number, y: number):number {
    return x + y;
}

//Falsey values:
//-------------
//undefined
//null
//0
//false
//""
//NaN
//
//Truthy
//----------
//"hello"
//"stuff"
//" " // NOTE: the space
//"0"
// 1
//  -1
//  -99999999999
//  {}
//  []
//  function(){}
//  true
//
//  5 == "5" true
//  5 === "5" false
//  null == undefined ture
//  null == false true
//null === undefined false
//  null === false false
//

let str = "hello"; //some user input
if (str !== null && str !== undefined && str !== "") {
    console.log('cool');
}
if (str) {
    console.log('cool');
}


function getAndDisplayName(event: Event){
    event.preventDefault(); //preventing page refresh from form
    let inputField= <HTMLInputElement>document.getElementById("inputName");
    let name = inputField.value;
    document.getElementById('display-name').innerHTML = `<h1>${name}, hello you sexy beast</h1>`;
}

try{
    getAndDisplayName();
} catch(ex){
    console.log('You have hit an error')
}
finally{
    console.log('Run this last')
}