/**
 * Created by matthewRanftle1 on 4/11/17.
 */
import { Injectable } from '@angular/core';
import {Http, Response, Headers, RequestOptions} from '@angular/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/toPromise';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/map';
import {UserResponse} from "../entities/userresponse";

@Injectable()
export class UserService {
  private userUrl = 'http://localhost:8000/api-token-auth/';
  private postResponse='';
  constructor(private _http: Http) { }

  private extractData(res: Response) {
    let body = res.json();
    return body.data || { };
  }

  login(username: string, password: string): Observable<Response>{
    let body = JSON.stringify({username: username, password: password});
    let headers = new Headers({ 'Content-Type': 'application/json' });
    let options = new RequestOptions({ headers: headers});
    const url = `${this.userUrl}/api/auth/login`;
    console.log(localStorage);
    return this._http.post(url, body, options)
      .map(this.extractData)
      .catch(this.handleError);

  }


  private handleError(error: any) {
    console.error('An error occurred', error);
    return Promise.reject(error.message || error);
  }
}