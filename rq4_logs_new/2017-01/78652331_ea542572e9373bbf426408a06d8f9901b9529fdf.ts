import { Component, OnInit, OnDestroy } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs/Rx';
import { SignUpService } from '../../header/signup-modal/signup.modal.service';
import * as _ from 'lodash';
import { AuthService } from '../../auth/auth.service';

import { LocalStorageService } from '../../auth/localStorage.service';

@Component({
  selector: 'app-first-step-signup',
  templateUrl: './first-step.component.html',
  styleUrls: ['./first-step.component.css']
})
export class FirstStepComponent implements OnInit, OnDestroy {
  public signupForm: any = {};
  public userErrorMessage: string;

  public firstStepSuccess: boolean = false;
  public isTypeArtist: boolean = false;
  public userType: any;
  public userProfile: any;

  public userProfileService: LocalStorageService;
  public signupServiceSubscribe: Subscription;
  public isEmailExistSignupServiceSubscribe: Subscription;
  public getLocationsSignupServiceSubscribe: Subscription;

  public signupService: SignUpService;
  public genres: any;
  private auth: AuthService;
  private router: Router;

  public constructor(signupService: SignUpService,
                     auth: AuthService,
                     userProfileService: LocalStorageService,
                     router: Router) {
    this.userProfileService = userProfileService;
    this.signupService = signupService;
    this.auth = auth;
    this.router = router;
  }

  public ngOnInit(): void {
  }

  public socialLogin(socialType): void {
    switch (socialType) {
      case 'google':
        this.auth.googleSignup();
        break;

      case 'facebook':
        console.log('Login via ', socialType);
        break;

      case 'twitter':
        console.log('Login via ', socialType);
        break;
    }
  }

  public isEmailExist(credentials: any): void {
    const signupData = {
      email: credentials.email,
      type: credentials.type,
      password: credentials.password
    };

    this.isEmailExistSignupServiceSubscribe = this.signupService.isEmailExist(signupData)
      .subscribe((res: any) => {
        const emailError: any = res.err;

        if (emailError) {
          this.signupForm.password = '';
          this.signupForm.passwordConfirm = '';
          this.userErrorMessage = emailError;
          return;
        }
        this.signupForm = {
          email: signupData.email,
          password: signupData.password,
          type: 'fan',
          country: '',
          city: '',
          genres: '',
          username: ''
        };
        this.userErrorMessage = '';
        this.firstStepSuccess = true;
      });
  }

  public submitData(): void {
    this.signupServiceSubscribe = this.signupService.signupUser(this.signupForm)
      .subscribe((): void => {
        this.auth.signUp(this.signupForm.email, this.signupForm.password);
        this.signupForm = {};
        this.userErrorMessage = '';
        this.firstStepSuccess = false;
      });
  }

  public ngOnDestroy(): void {
    this.getLocationsSignupServiceSubscribe.unsubscribe();
    this.isEmailExistSignupServiceSubscribe.unsubscribe();
    this.signupServiceSubscribe.unsubscribe();
  }
}