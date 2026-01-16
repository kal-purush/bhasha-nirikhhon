import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable } from 'rxjs/Rx';
import 'rxjs/add/operator/map';

import { Event } from '../models/event';

/*
  Generated class for the WdiconfEvents provider.

  See https://angular.io/docs/ts/latest/guide/dependency-injection.html
  for more info on providers and Angular 2 DI.
*/
@Injectable()
export class WdiconfEvents {
  wdiconfEventsApiUrl = "https://wdiconfapi.herokuapp.com/api/events";

  constructor(public http: Http) {
    console.log('Hello WdiconfEvents Provider');
  }

  loadForPresenter(id: number): Observable<Event[]> {
    return this.http.get(`${this.wdiconfEventsApiUrl}?presenter_id=${id}`)
      .map(res => <Event[]>res.json().results);
  }

}