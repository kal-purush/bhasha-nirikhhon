import { printObject, genericFunction, genericFunctionArrow } from "../generics/generics";

import { Villain } from "../interfaces/villain";
import { Hero } from "../interfaces/hero";


// printObject(123);
// printObject(new Date());
// printObject([1,2,3,4,5,6]);
// printObject(['a','b','c']);

// console.log(genericFunctionArrow(3.1416345765).toFixed(4));
// console.log(genericFunctionArrow('hola mundo').toUpperCase());

const deadpool ={
    name:'Deadpool',
    realName: 'Wade Wiston Wilson',
    dangerLevel: 130
}

console.log(genericFunctionArrow<Villain>(deadpool).dangerLevel);