import { Router } from '@angular/router';
import { HeadersService } from './headers-service';
import { Headers, Http } from '@angular/http';
import { Injectable } from '@angular/core';
import { ServerDomain }   from '../models/server-domain';
import 'rxjs/add/operator/toPromise';

@Injectable()
export class ConversationService {
  serverdomain: ServerDomain = new ServerDomain();
  headersService: HeadersService = new HeadersService();
  constructor(private http: Http, private router: Router) {
  }

  private handleError(error: any): Promise<any> {
    console.error('An error occurred', error);
    let userInfo = localStorage.getItem('user');
    if (error.status === 401 && userInfo != null) {
      alert('Your token is expired');
      localStorage.removeItem('user');
      this.router.navigate(['sign-in', 'admin']);
    }
    return Promise.reject(error.message || error);
  }

  create(data: Object): Promise<any> {
    let requestUrl = this.serverdomain.domain + '/conversations';
    let headers = new Headers();
    this.headersService.createAuthHeaders(headers);
    var auth = this.headersService.createAuthParams();
    return this.http
      .post(requestUrl, JSON.stringify(Object.assign(data, auth)), {headers: headers})
      .toPromise()
      .then(res => { return res.json().data;})
      .catch(error => { return this.handleError(error);});
  }
}