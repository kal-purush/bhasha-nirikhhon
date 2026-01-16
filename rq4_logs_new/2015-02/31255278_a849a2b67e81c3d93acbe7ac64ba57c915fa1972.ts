// LICENSE : MIT
"use strict";
class Person {
    constructor(public name:string, public age:number) {

    }
}
function createPerson(personClass:{ new (name:string, age:number): Person;}) {
    return new personClass("vvaka", 229);
}
var person = createPerson(Person);