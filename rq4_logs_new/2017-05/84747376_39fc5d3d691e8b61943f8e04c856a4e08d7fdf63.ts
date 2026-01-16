import { Injectable } from '@angular/core';
import { Headers, Http, Response } from '@angular/http';
import { Patient } from './patient';

import 'rxjs/add/operator/toPromise';

@Injectable()
export class AuthService {

  private url = 'http://localhost:3000';

  constructor(private http: Http) { }

  logIn(patient: Patient): Promise<Response> {
    const headers = new Headers({
      'Content-Type': 'application/json'
    });

    return this.http
      .post(this.url + '/patients/login', JSON.stringify(patient), { headers: headers })
      .toPromise()
      .then(res => {
        return res;
      })
      .catch(this.handleError);
  }

  private handleError(error: any): Promise<any> {
    console.error('An error occurred', error);
    return Promise.reject(error.message || error);
  }
}