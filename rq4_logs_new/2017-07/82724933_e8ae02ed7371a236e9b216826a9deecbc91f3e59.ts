import {Injectable} from '@angular/core';
import {Headers, Http, RequestOptions, Response} from '@angular/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/toPromise';
import 'rxjs/add/operator/catch';
import 'rxjs/add/observable/throw';

@Injectable()
export class KeepersService {
  private url = 'http://progress.juja.com.ua/api/keepers';
  private headers = new Headers({'Content-Type': 'application/json'});

  constructor(private http: Http) { }

  private extractData(res: Response): Array<any> {
    const body = res.json();
    return body || {};
  }

  getActiveKeepers(): Observable<any> {
    const options = new RequestOptions({headers: this.headers});
    return this.http.get(this.url, options)
      .map(this.extractData)
      .catch((error: any) => {return Observable.throw(error); });
  }

}