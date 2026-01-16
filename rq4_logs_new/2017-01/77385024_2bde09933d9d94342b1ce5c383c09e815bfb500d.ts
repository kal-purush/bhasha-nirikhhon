import { Injectable } from '@angular/core';
import { Headers, Http, RequestOptions, Response } from '@angular/http';

import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

import { Event } from './../models/events';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class EventsService {
    projects: Event[] = [];

    private eventsUrl = 'api/events';

    constructor(private http: Http) {

    }

    getAll(): Observable<any> {
      return this.http.get(this.eventsUrl)
                      .map((res: Response) => res.json().data as Event[]);
    }
}