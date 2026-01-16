"use strict";
// Declare a variable with it data type
let username; // Means username can only be string
let age; // Means age can only be number
let isActive; // ...boolean
let car; // Means that car can be any of the data types
let isDoctor; // Means isDcotor can be only number or a boolean
/*
username = 48 // Error occured because username is a number and must only accept string data
*/
username = "user";
age = 90;
isActive = true;
car = null;
car = "Toyota";
car = 45;
car = false;
/*
car can be assign to any of the data types because it's any
*/
console.log(username, age, isActive, car); // user 90 true false
isDoctor = 0;
isDoctor = true;
/*
Now, isDoctor has been assigned to 0 and later reassigned to true
Because isDcotor can have either a number or a boolean
*/
console.log(username, age, isActive, car, isDoctor); // user 90 true false true