import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Global } from '../global';
import { Observable } from "rxjs";

@Injectable({
  providedIn: 'root'
})
export class UserService {
  url:string;

  constructor(private http:HttpClient) { 
      this.url=Global.url;
  }

  getUsers():Observable<any>{
    let headers= new HttpHeaders().set('Content-Type','application/json');

    return this.http.get(this.url+'usersInfo',{headers:headers});

  }
}