import { Component, OnInit, OnDestroy } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs/Rx';
import * as _ from 'lodash';

import { SignUpService } from '../../header/signup-modal/signup.modal.service';
import { AuthService } from '../../auth/auth.service';
import { SearchService } from '../../shared/search/search.service';
import { LocalStorageService } from '../../auth/localStorage.service';

@Component({
  selector: 'app-second-step-signup',
  templateUrl: './second-step.component.html',
  styleUrls: ['./second-step.component.css']
})
export class SecondStepComponent implements OnInit, OnDestroy {
  public signupForm: any = {};
  public userErrorMessage: string;
  public countries: any;
  public cities: any;
  public isTypeArtist: boolean = false;
  public userType: any;
  public userProfile: any;

  public userProfileService: LocalStorageService;
  public signupServiceSubscribe: Subscription;
  public isEmailExistSignupServiceSubscribe: Subscription;
  public getLocationsSignupServiceSubscribe: Subscription;
  public searchService: SearchService;
  public searchServiceSubscribe: Subscription;
  public genres: any;
  public signupService: SignUpService;
  private auth: AuthService;
  private router: Router;

  public constructor(signupService: SignUpService,
                     auth: AuthService,
                     searchService: SearchService,
                     userProfileService: LocalStorageService,
                     router: Router) {
    this.userProfileService = userProfileService;
    this.signupService = signupService;
    this.searchService = searchService;
    this.auth = auth;
    this.router = router;
  }

  public ngOnInit(): void {
    const userProfile: any = this.userProfileService.getItem('profile');

    this.getLocationsSignupServiceSubscribe = this.signupService.signupGetLocations()
      .subscribe((res): void => {
        const locations = res.data;
        this.countries = locations.getCountries;
        this.cities = locations.getCities;
      });

    this.searchServiceSubscribe = this.searchService.getMusicStyles()
      .subscribe((res: any): void => {
        const styles: any[] = res.data;
        this.genres = styles[0].genres;
      });

    if (userProfile) {
      this.userProfile = JSON.parse(userProfile);
      this.signupForm.username = userProfile.name;
      this.signupForm.country = userProfile.country;
    }

    this.userProfileService.getItemEvent().subscribe((userData) => {
      this.userProfile = JSON.parse(userData.value);
      this.signupForm.username = this.userProfile.name;
      this.signupForm.country = this.userProfile.country;
    });
  }

  public isUserArtist(type: boolean): void {
    this.userType = {type: type ? 'artist' : 'fan'};
    this.isTypeArtist = type;
    _.extend(this.signupForm, this.userType);
  }

  public submitData(): void {
    this.signupForm = {
      email: this.userProfile.email,
      type: this.signupForm.type,
      country: this.userProfile.country || this.signupForm.country,
      city: this.userProfile.city || this.signupForm.city,
      genres: this.signupForm.genres,
      username: this.signupForm.username,
      gender: this.signupForm.gender,
      groupName: this.signupForm.groupName
    };
    this.signupServiceSubscribe = this.signupService.signupUser(this.signupForm)
      .subscribe((): void => {
        this.router.navigate(['/home']);
      });
  }

  public ngOnDestroy(): void {
    if (this.getLocationsSignupServiceSubscribe) {
      this.getLocationsSignupServiceSubscribe.unsubscribe();
    }

    if (this.isEmailExistSignupServiceSubscribe) {
      this.isEmailExistSignupServiceSubscribe.unsubscribe();
    }

    if (this.signupServiceSubscribe) {
      this.signupServiceSubscribe.unsubscribe();
    }
  }
}