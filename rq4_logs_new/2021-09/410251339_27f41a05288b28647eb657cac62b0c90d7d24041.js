"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
// Basic Types /////////////////////////////////////////////////////////
var id = 5;
var company = "Traversy Media";
var isPublished = true;
var x = "Hello";
var ids = [1, 2, 3, 4, 5];
var arr = [1, "yes", true];
// Tuple - is a TypeScript type that works like an array with some special considerations ///////////////////////////
var person = [1, "2", false];
// Tuple Array
var employee;
employee = [
    [1, "Brad"],
    [2, "Lawrence"],
];
// Union ///////////////////////////////////////////////////////////////////
var pid;
pid = "22";
pid = 22;
// Enum /////////////////////////////////////////////////////////////////////
var Direction1;
(function (Direction1) {
    Direction1[Direction1["Up"] = 0] = "Up";
    Direction1[Direction1["Down"] = 1] = "Down";
    Direction1[Direction1["Left"] = 2] = "Left";
    Direction1[Direction1["Right"] = 3] = "Right";
})(Direction1 || (Direction1 = {}));
var Direction2;
(function (Direction2) {
    Direction2[Direction2["Up"] = 1] = "Up";
    Direction2[Direction2["Down"] = 2] = "Down";
    Direction2[Direction2["Left"] = 3] = "Left";
    Direction2[Direction2["Right"] = 4] = "Right";
})(Direction2 || (Direction2 = {}));
var Direction3;
(function (Direction3) {
    Direction3["Up"] = "Up";
    Direction3["Down"] = "Down";
    Direction3["Left"] = "Left";
    Direction3["Right"] = "Right";
})(Direction3 || (Direction3 = {}));
var user = {
    id: 1,
    name: "John",
};
// Type Assertion ////////////////////////////////////////////////////////////////
var cid = 1;
// two ways to do it
// first one
// let customerId = <number>cid;
// second one
var customerId = cid;
// Functions ///////////////////////////////////////////////////////////////////
function addNum(x, y) {
    return x + y;
}
console.log(addNum(2, 3));
// Void - can use if function does not return any value ///////////
function log(message) {
    console.log(message);
}
var user1 = {
    id: 1,
    name: "John",
};
var add = function (x, y) { return x + y; };
// Classes /////////////////////////////////////////////////////////////
var Person = /** @class */ (function () {
    function Person(id, name) {
        this.id = id;
        this.name = name;
    }
    Person.prototype.register = function () {
        return this.name + " is now registred";
    };
    return Person;
}());
var brad = new Person(1, "Brad");
var mike = new Person(2, "Mike");
var Human = /** @class */ (function () {
    function Human(id, name) {
        this.id = id;
        this.name = name;
    }
    Human.prototype.register = function () {
        return "Hey, I am " + this.name;
    };
    return Human;
}());
var franky = new Human(3, "Franky");
// Extending Classes (Subclasses) ///////////////////////////////////////////
var Employee = /** @class */ (function (_super) {
    __extends(Employee, _super);
    function Employee(id, name, position) {
        var _this = _super.call(this, id, name) || this;
        _this.position = position;
        return _this;
    }
    return Employee;
}(Person));
var emp = new Employee(5, "Dimi", "Software Developer");
console.log(emp.register());
// Generics /////////////////////////////////////////////////////////////////
function getArray(items) {
    return new Array().concat(items);
}
var numArray = getArray([1, 2, 3, 4]);
var strArray = getArray(["Brad", "John", "Jill"]);
numArray.push(1);
console.log(numArray);
/////////////////////////////////////////////////////////////////////////
// React /// React /// React /// React /// React /// React /// React ////
/////////////////////////////////////////////////////////////////////////
// How to run TypeScript in React
// write in terminal
// npx create-react-app . --template typescript