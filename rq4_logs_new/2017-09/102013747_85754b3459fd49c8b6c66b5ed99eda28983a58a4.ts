import { Injectable } from '@angular/core';
import {Http, RequestOptions, Response} from "@angular/http";
import {Observable} from "rxjs/Observable";
import 'rxjs/add/operator/map';

@Injectable()
export class UniversityService {

  constructor(private _http: Http) { }

  createUniversity(apiUrl,data): Observable<any[]> {
    return this._http.post(apiUrl,data).map((res:Response) => res.json());
  }

  addLocation(apiUrl,data): Observable<any[]>{
    return this._http.post(apiUrl,data).map((res:Response) => res.json());
  }

  uploadFile(apiUrl,data,options): Observable<any[]>{
    return this._http.post(apiUrl,data,options).map((res:Response) => res.json());
  }
}