import { Http, Response, Headers } from '@angular/http';
import 'rxjs/add/operator/map'
import { Observable } from 'rxjs/Observable';
import {Injectable} from '@angular/core';

@Injectable()
export class AirportService { 
  private actionUrl: string;
  constructor(private _http: Http) {
    this.actionUrl = 'http://localhost:5000/api/airports/CMH/';
}

public GetAll = (): Observable<any> => {
    return this._http.get(this.actionUrl)
        .map((response: Response) => <any>response.json())
        .do(x => console.log(x));
}
}