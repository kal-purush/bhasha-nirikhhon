import { Component, OnInit } from '@angular/core';
import { Validators, FormGroup, FormBuilder } from '@angular/forms';
import { User } from 'src/app/Models/User';
import { UsersService } from 'src/app/Services/users.service';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']
})

export class LoginComponent implements OnInit {
  loginForm: FormGroup;
  user = new User();

  constructor(
    private formBuilder: FormBuilder, 
    private userService: UsersService) {
  }

  ngOnInit() {
    this.createForm();
  }

  submit() {
    this.user.UserName = this.loginDetails.userName.value;
    this.user.Password = this.loginDetails.password.value;
    this.userService.verifyUser(this.user);
  }

  createForm() {
    this.loginForm = this.formBuilder.group({
      userName: ['', Validators.required],
      password: ['', Validators.required, Validators.minLength(5)]
    });
  }

  get loginDetails() { return this.loginForm.controls}

}