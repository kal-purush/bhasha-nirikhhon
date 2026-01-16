/**
 * Created by matthewRanftle1 on 4/11/17.
 */
import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';

import 'rxjs/add/operator/toPromise';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/map';

@Injectable()
export class UserService {
  private userUrl = 'http://localhost:8000/api-token-auth';

  constructor(private http: Http) { }

  getTokenWordPairs() {
    return this.http.get(this.userUrl)
      .toPromise()
      .then(response => response.json())
      .catch(this.handleError);
  }

  private handleError(error: any) {
    console.error('An error occurred', error);
    return Promise.reject(error.message || error);
  }

}