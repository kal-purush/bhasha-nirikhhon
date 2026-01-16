import {Component, OnInit} from '@angular/core';
import {Employee} from './employee';
import {EmployeeService} from './emp.service';

@Component({
    selector: 'employee',
    templateUrl: '../app/employee/employee-list.html',
    providers: [EmployeeService]
})
export class EmployeeComponent implements OnInit {

    public employees: Employee[];

    constructor(private employeeService:EmployeeService){
        console.log("Employee Service constructor..");
        
    }

    ngOnInit(){
        this.loadEmployees();
        console.log(this.employees);
        
    }

    loadEmployees(){
        this.employees = this.employeeService.getEmployees();
    }

}