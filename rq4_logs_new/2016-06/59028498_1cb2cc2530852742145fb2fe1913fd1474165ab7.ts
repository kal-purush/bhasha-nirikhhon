import { UserStore } from '../../common/stores/user.store';
import { identifier, Logger } from '@ubiquits/core/common';
import { User } from '../../common/models/user.model';

import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class UserHttpStore extends UserStore {

  private endpoint: string = 'api/users';

  constructor(private http: Http, protected logger:Logger) {
    super();
  }

  public findOne(id: identifier): Promise<User> {
    
    return this.http.get(`${this.endpoint}/${id}`)
      .toPromise()
      .then(this.extractData)
      .catch((error) => this.handleError(error));

  }

  private extractData(res: Response): User {
    let body = res.json();
    return new User(body);
  }

  private handleError(error: any) {
    let errMsg = (error.message) ? error.message : error.status ? `${error.status} - ${error.statusText}` : 'Server error';
    this.logger.error(errMsg);
    return Promise.reject(errMsg);
  }

}