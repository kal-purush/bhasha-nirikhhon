import { Injectable } from '@angular/core';
import { Http, RequestOptions } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import { URL} from '../app.routes';

@Injectable()
export class LogoutService {
  public apiURL;
  constructor(private http: Http) {
    this.apiURL = URL + 'api/sign_out';
  }

  logout(token: string): Observable<any> {
    const headers: any = {'RT-AUTH-TOKEN': token};  
    const option = new RequestOptions({headers: headers})
    console.log(option);
    return this.http.delete(this.apiURL, option);
  }
}