import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class HistoryUsePointService {

  public API: string = 'http://localhost:8433/api/v1/point';

  constructor(public http: HttpClient) {
  }

  getPointAccount(id:number):Observable<any>{
    return this.http.get(`${this.API}/${id}`);
  }

  getSearchPointAccount(id:number,from:string,to:string,checkPoint:string):Observable<any>{
    return this.http.get(`${this.API}/${id}/${from}/${to}/${checkPoint}`);
  }
}