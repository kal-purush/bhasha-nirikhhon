import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Http, Headers } from '@angular/http';
import { contentHeaders } from '../common/headers';
import { User } from '../user';
import { Employee } from '../employee';
import { DataService } from '../service/dataservice';
import { Location } from '../location';

const styles = require('./addemployee.css');
const template = require('./addemployee.html');

@Component({
  selector: 'addmovie',
  template: template,
  styles: [styles]
})
export class AddEmployee {
  public location: Location;
  public employee: Employee;
  public object: any;
  public validForm = true;
  constructor(public router: Router, public http: Http, public dataservice: DataService) { }
  adminEmployeeServices(event) {
    this.router.navigate(['adminemployeeservices']);
  }


  addEmployee(event, firstname, lastname, address, city, state, zipcode,
    password, ssn, telephone, hourlyrate, startdate) {
    this.location = {
      state: state,
      city: city,
      zipcode: parseInt(zipcode)
    };
    this.employee = {
      address: address,
      firstname: firstname,
      lastname: lastname,
      location: this.location,
      password: password,
      ssn: parseInt(ssn),
      telephone: parseInt(telephone)
    };
    this.object = {
      employee: this.employee,
      hourlyrate: parseInt(hourlyrate),
      id: parseInt(ssn),
      startdate: startdate
    };

    console.log(this.object);
    this.postEmployee();
  }

  postEmployee() {
    var authHeader = new Headers();
    authHeader.append('Authorization', 'Basic ' +
      btoa(this.dataservice.username + ':' + this.dataservice.password));
    authHeader.append('Content-Type', 'application/json');
    let body = this.object;
    event.preventDefault();
    this.http.post('http://localhost:8080/storage/manager/employee',
      body, { headers: authHeader })
      .subscribe(
      response => {
        console.log(response.json());
        alert('Created');
      },
      error => {
        console.log(error.text());
        alert(`{
          "employee": {
              "address": "string",
              "firstname": "string",
              "lastname": "string",
              "location": {
                    "city": "string",
                    "state": "string",
                    "zipcode": number
              },
              "password": "string",
              "ssn": number,
              "telephone": "string"
          },
          "hourlyrate": number,
          "id": number,
          "ssn": number,
          "startdate": "date"
          }`);
      }
      );
  }
}
