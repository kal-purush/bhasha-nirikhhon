import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { Account } from 'src/app/model/Account';
import { TravelServiceService } from 'src/app/service/travel-service.service';
import { LoginAuthService } from '../../services/login-auth.service';

@Component({
  selector: 'app-register-form',
  templateUrl: './register-form.component.html',
  styleUrls: ['./register-form.component.css']
})
export class RegisterFormComponent implements OnInit {

  userAlreadyExists: boolean = false;

  userAccounts: Array<Account> = [];

  form: FormGroup;
  constructor(private readonly formBuilder: FormBuilder,
              private authService: LoginAuthService,
              private tservice: TravelServiceService,
              private router: Router) { 
    this.form = this.formBuilder.group({
      username: ['', [Validators.required, Validators.minLength(2), Validators.maxLength(15)]],
      userPassword: ['', [Validators.required, Validators.minLength(8), Validators.maxLength(12), this.validatePassword]]
    });
  }

  validatePassword(fc: FormControl) {
    // const value = fc.value as string;
    // const isInvalid = 'pa'
  }

  ngOnInit(): void {
  }

  get controls() { return this.form.controls; }

  submitForm(): void {
    this.userAlreadyExists = false;
    if (this.form.valid) {
      this.tservice.getUserLoginInput().subscribe(userAccounts => this.userAccounts = userAccounts); // First get the list of users from backend.
      console.log(this.userAccounts);
      if (this.userAccounts.find((user) => { // Then check if user is in that list.
        // return (this.authService.loginAuthenticated("melogin", this.controls['username'].value, 
        //                                             "mepassword", this.controls['userPassword'].value));
        return (this.authService.loginAuthenticated(user.username, this.controls['username'].value, 
                                                    user.password, this.controls['userPassword'].value));
      }) === null) { // If user doesn't exist, post the new user and automatically sign user in.
        console.log("Successful user creation");
        sessionStorage.setItem('signedIn', 'true');
        sessionStorage.setItem('token', this.controls['username'].value);
        this.tservice.postUserLoginInput(this.controls['username'].value, this.controls['userPassword'].value);
        // console.log(this.form.getRawValue());
        this.router.navigate(['/packages']); // After user is verified to exist in database.
      } else {
        console.log("The user already exists");
        this.userAlreadyExists = true;
      }
    }
    else {
      console.log('Input not verified');
    }
  }
}