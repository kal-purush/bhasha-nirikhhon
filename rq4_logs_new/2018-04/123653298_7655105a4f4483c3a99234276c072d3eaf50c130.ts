import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { HttpClient, HttpParams } from '@angular/common/http';
import { of } from 'rxjs/observable/of';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';
import { User } from './models/user';
import { environment } from '../environments/environment';
import { ToasterService } from './toaster.service';

@Injectable()
export class MeService {

  url: string = environment.apiUrl + "/me";

  constructor(private http: HttpClient, public toaster: ToasterService) { }


  getUser(): Observable<User> {
     return this.http.get(this.url).map(data => {
         return data;
     }).catch((error) => {
         this.toaster.showError(error.error);
         return Observable.throw(error);
     });
  }

  updateUser(user: User): Observable<User> {
    return this.http.put(this.url, user).map(data => {
         return data;
     }).catch((error) => {
         this.toaster.showError(error.error);
         return Observable.throw(error);
     });
  }

}