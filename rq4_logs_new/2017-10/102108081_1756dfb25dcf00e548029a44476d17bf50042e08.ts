import { User } from "./user.model";
import { Http,Response,Headers } from "@angular/http";
import { Injectable,EventEmitter } from "@angular/core";
import {Observable} from "rxjs";

@Injectable()
export class AuthService{
  
  constructor(private http:Http){}

  signup(user:User){
    const body = JSON.stringify(user)
    const headers = new Headers({'Content-type':'application/json'});
    return this.http.post('http://localhost:3000/user',body,{headers:headers})
      .map((response: Response) => response.json())
      .catch((error: Response) => Observable.throw(error.json()));
  }
}