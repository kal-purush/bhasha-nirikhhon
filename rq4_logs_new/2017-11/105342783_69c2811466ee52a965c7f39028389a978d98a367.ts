import {Component, EventEmitter, Injectable, OnChanges, Output} from '@angular/core';
import {Location} from '../../ts models/location.model';

@Injectable()
export class UploadService implements OnChanges  {

  public NewLocation: Location = new Location(
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
  );


  constructor() {
      if (localStorage.getItem('location'))   {
        this.NewLocation = JSON.parse(localStorage.getItem('location'));
    } else {
        if (localStorage.getItem('userID'))  {
            this.NewLocation = new Location(localStorage.getItem('userID'));
        }   else {
            // this.authService.login();
        }
    }
  }

    ngOnChanges() {
      localStorage.setItem('location', JSON.stringify(this.NewLocation));
    }
}