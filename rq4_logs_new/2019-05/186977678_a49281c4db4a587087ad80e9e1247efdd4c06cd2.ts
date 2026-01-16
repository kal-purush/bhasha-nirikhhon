import { Component, OnInit } from '@angular/core';
import { UserService } from '../../services/user.service';
import { Location } from '@angular/common';
import { Router } from '@angular/router';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { UserModel } from '../../models/user.model';

@Component({
  selector: 'app-signup',
  templateUrl: './signup.component.html'
})
export class SignupComponent implements OnInit {
  signupForm: FormGroup;
  submitted = false;

  constructor(
    private router: Router,
    private userService: UserService,
    private fb: FormBuilder,
    private location: Location
  ) {}

  ngOnInit() {
    this.formBuilderInit();
  }

  formBuilderInit(): void {
    this.signupForm = this.fb.group({
      email: ['', Validators.required, Validators.email],
      password: [
        '',
        Validators.required,
        Validators.minLength(6),
        Validators.maxLength(12)
      ]
    });
  }

  back(): void {
    this.location.back();
  }

  onSubmit(): void {
    // You can validate all your fields here before sending loginForm
    this.sendSignupForm();
  }

  get form() {
    return this.signupForm.controls;
  }

  private sendSignupForm(): void {
    this.submitted = true;
    if (this.signupForm.invalid) {
      return;
    }

    const user = this.signupForm.value as UserModel;
    this.userService.signup(user).subscribe(response => {
      this.router.navigate(['/login']);
    });
  }
}