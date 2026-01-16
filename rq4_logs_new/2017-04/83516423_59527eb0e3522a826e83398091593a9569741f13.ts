import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Http, Headers } from '@angular/http';
import { contentHeaders } from '../common/headers';
import { User } from '../user';
import { Customer } from '../customer';
import { DataService } from '../service/dataservice';
import { Location } from '../location';

const styles = require('./addcustomer.css');
const template = require('./addcustomer.html');

@Component({
  selector: 'addcustomer',
  template: template,
  styles: [styles]
})
export class AddCustomer {
  public location: Location;
  public customer: Customer;
  public object: any;
  public validForm = true;
  constructor(public router: Router, public http: Http, public dataservice: DataService) { }
  employeeCustomerServices(event) {
    this.router.navigate(['employeecustomerservices']);
  }


addCustomer($event, firstname, lastname,
  address, city, state, creditcardnumber,
  zipcode, password, ssn, telephone, email, rating ) {
    this.location = {
      state: state,
      city: city,
      zipcode: parseInt(zipcode)
    };
    this.customer = {
      address: address,
      firstname: firstname,
      lastname: lastname,
      location: this.location,
      password: password,
      ssn: parseInt(ssn),
      telephone: parseInt(telephone)
    };
    this.object = {
      customer: this.customer,
      email: email,
      id: parseInt(ssn),
      creditcardnumber: creditcardnumber,
      rating: rating
    };

    console.log(this.object);
    this.postCustomer();
  }

  postCustomer() {
    var authHeader = new Headers();
    authHeader.append('Authorization', 'Basic ' +
      btoa(this.dataservice.username + ':' + this.dataservice.password));
    authHeader.append('Content-Type', 'application/json');
    let body = this.object;
    event.preventDefault();
    this.http.post('http://localhost:8080/storage/employee/customer',
      body, { headers: authHeader })
      .subscribe(
      response => {
        console.log(response.json());
        alert('Created');
      },
      error => {
        console.log(error.text());
        alert(`Please Make sure your id is valid
        {
                    "creditcardnumber": "string",
                    "customer": {
                      "address": "string",
                      "firstname": "string",
                      "lastname": "string",
                      "location": {
                        "city": "string",
                        "state": "string",
                        "zipcode": 0
                      },
                      "password": "string",
                      "ssn": 0,
                      "telephone": "string"
                    },
                    "email": "string",
                    "id": 0,
                    "rating": 0
                  }`);
      }
      );
  }
}
