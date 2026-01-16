"use strict";

class Employee {
    constructor(name, department, officeLocation, ) {
        this._name = name;
        this._department = department;
        this._officeLocation = officeLocation;
    }
    getFirstName() {
        var names = this._name.split();
        return names[0];
    }
    getLastName() {
        var names = this._name.split();
        return names[-1];
    }
    getFullName() {
        return this._name
    }
}

class AdminDpt extends Employee {
    constructor(name, department, officeLocation, workDays) {
        super(name, department, officeLocation, workDays);
        this._employeesWithManager = [];
    }
    addEmployee(employee) {
        this._employeesWithManager.push(employee)
    }
}
class InfoTech extends Employee {
    constructor(name, department, officeLocation, workDays) {
        super(name, department, officeLocation, workDays);
    }
    getFunFact(hobby) {
        let x = new Employee();
        let firstName = x['getFirstName']();
        return first + 'loves' + hobby
    }

}
class HumanResources extends Employee {
    constructor(name, department, officeLocation, workDays, ) {
        super(name, department, officeLocation, workDays);

    }
    getNamesOfActiveEmpl() {
        for (let value of this._employeesWithManager) {
            value += 1;
            return value;
        }
    }
}