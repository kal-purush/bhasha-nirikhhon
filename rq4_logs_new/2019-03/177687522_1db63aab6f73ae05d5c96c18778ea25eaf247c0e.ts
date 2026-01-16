import { HttpParams } from '@angular/common/http';
import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { SpringCloudLoginService } from './spring-cloud-login.service';


@Component({
  selector: 'app-spring-cloud-login',
  templateUrl: './spring-cloud-login.component.html',
  styleUrls: ['./spring-cloud-login.component.scss']
})
export class SpringCloudLoginComponent implements OnInit {

  loginForm: FormGroup;
  invalidLogin: boolean = false;
  loginDate: any = {
    username: 'user',
    password: 'password'
  }

  constructor(
    private formBuilder: FormBuilder,
    private router: Router,
    private springCloudLoginService: SpringCloudLoginService) { }

  onSubmit() {
    if (this.loginForm.invalid) {
      return;
    }
    const body = new HttpParams()
      .set('username', this.loginForm.controls.username.value)
      .set('password', this.loginForm.controls.password.value)
      .set('grant_type', 'password');

    this.springCloudLoginService.login(body.toString()).subscribe(data => {
      window.sessionStorage.setItem('token', JSON.stringify(data));
      console.log(window.sessionStorage.getItem('token'));
      this.router.navigate(['example/employees']);
    }, error => {
      alert(error.error.error_description)
    });
  }

  ngOnInit() {
    window.sessionStorage.removeItem('token');
    this.loginForm = this.formBuilder.group({
      username: [this.loginDate.username, Validators.compose([Validators.required])],
      password: [this.loginDate.password, Validators.required]
    });
  }

}