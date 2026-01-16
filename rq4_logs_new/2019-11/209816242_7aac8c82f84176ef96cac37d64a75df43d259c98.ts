/**
 * @author Andreas Gøricke, s153804
 */

import { Injectable, PACKAGE_ROOT_URL } from '@angular/core';
import { EventToCreate } from '../models/calendar/event-to-create.model';
import { Event } from '../models/calendar/event.model';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { BaseService } from './base.service';

@Injectable({
    providedIn: 'root'
  })
  
  export class EventService extends BaseService {
  
    constructor(http: HttpClient) {
      super(http);
    }

createEvent(event: EventToCreate) {
    const options = {
      headers: new HttpHeaders({
          'Content-Type':  'application/json',
      })
    };

    return this.http.post<EventToCreate>(`${this.backendBaseUrl}/api/v1/event/create`, event, options);
  }

  getAll() {
    return this.http.get<Event[]>(`${this.backendBaseUrl}/api/v1/event/getall`);
  }

 

}