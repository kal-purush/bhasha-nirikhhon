import { Http, Response, Headers } from '@angular/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch'

@Injectable()
export class BaconService {
  private url:string;

  constructor(private _http: Http) {
    this.url = "https://baconipsum.com/api/";
   }

   public GetData = (type:string, paras:number): Observable<any> => {
        let predicate = "?";
        predicate += "type=" + type;
        predicate += "&paras=" + paras;

     return this._http.get(this.url+predicate)
            .map((response: Response) => <any>response.json())
            .catch((error:any) => Observable.throw(error.json().error || 'Server error'));
   }
}