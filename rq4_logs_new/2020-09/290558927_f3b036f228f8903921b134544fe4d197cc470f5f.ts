import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class PerfAppService {
public route:String;
public method:String;
public requestBody:any={};
  constructor(private http: HttpClient) { }

   CallAPI() {    
    return this.http
      .post<any>(this.getRoute(), this.requestBody);
  }

  getRoute(){
    return `${environment.ApiPath}${this.route}/${this.method}`;
  }
}