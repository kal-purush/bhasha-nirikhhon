import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from "@angular/common/http";
import {User} from "../models/user";

@Injectable({
  providedIn: 'root'
})
export class RegistrationService {

  constructor(private http: HttpClient) {
  }

  register(user: User) {
    console.log('register: ' + JSON.stringify(user));
    let url = 'http://localhost:8888/registration';
    let header = new HttpHeaders({'Content-Type': 'application/json'});
    return this.http.post(url, JSON.stringify(user), {headers: header});
  }

}