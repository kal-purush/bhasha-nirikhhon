import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Notification } from './../models/notification';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
@Injectable()
export class NotificationClientService {
  private baseURL = 'https://project-9-20-187720.appspot.com/';
  constructor( private http: HttpClient ) { }

  public getAllNotifications(): Observable<Notification[]> {
    const url = this.baseURL + 'notifications';
    return this.http.get(url).map( res => {
      return res['Notifications'].map(item => {
        return new Notification(
          item.tittle,
          item.message,
          item.category,
          item.resource_id,
          item.name,
          new Date(item.date)
        );
      });
    });
  }

}