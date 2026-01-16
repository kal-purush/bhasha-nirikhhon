"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var getFullName = function (_a) {
    var firstName = _a.firstName, lastName = _a.lastName;
    return "".concat(firstName, " ").concat(lastName);
};
var person = getFullName({ firstName: "Enggar", lastName: "Rahayu" });
console.log(person);