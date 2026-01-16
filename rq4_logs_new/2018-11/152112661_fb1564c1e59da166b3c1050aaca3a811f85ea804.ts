import { Component, OnInit } from '@angular/core';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import {RegisterEmployeeModel} from '../app.component';
import {AuthService} from '../auth.service';

@Component({
  selector: 'app-register-employee',
  templateUrl: './register-employee.component.html',
  styleUrls: ['./register-employee.component.css']
})
export class RegisterEmployeeComponent implements OnInit {
  private registerEmployeeForm: FormGroup;
  constructor(private formBuilder: FormBuilder,
              private authService: AuthService) {

  }

  ngOnInit() {
    this.registerEmployeeForm = this.formBuilder.group({
      firstName: ['', Validators.pattern('[A-ZŹĄĘÓŁŻ]{1}[a-z,ąęółńćźż]{2,}')],
      lastName: ['', Validators.pattern('[A-ZŹĄĘÓŁŻ]{1}[a-z,ąęółńćźż]{2,}')],
      email: ['', Validators.pattern('^[A-Za-z0-9._-]{1,}@[a-z]{1,6}.[a-z]{2,3}$')],
      password: ['', Validators.pattern('[A-Za-z0-9ĄŻŹÓŁĘążźćńłóę!@#%*\^]{6,20}')],
      confirmPassword: ['', Validators.pattern('[A-Za-z0-9ĄŻŹÓŁĘążźćńłóę!@#%*\^]{6,20}')],
      hireDate: ['', Validators.required]
    });
  }

  get f() {
    return this.registerEmployeeForm.controls;
  }

  submit() {
    const date = new Date(this.f.hireDate.value);
    const registerEmployeeModel: RegisterEmployeeModel = {
      firstName: this.f.firstName.value,
      lastName: this.f.lastName.value,
      email: this.f.email.value,
      password: this.f.password.value,
      confirmPassword: this.f.confirmPassword.value,
      hireDate: date.getDate() + '-' + (date.getMonth() + 1) + '-' + date.getFullYear(),
      accessToken: this.authService.getAccessToken()
    };
    this.authService.registerEmployee(registerEmployeeModel);
  }

  /* checkData(): boolean {
    let result = this.f.firstName.errors.pattern;

  } */
}