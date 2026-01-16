import {Injectable} from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable} from "rxjs";
import {IUser} from "../models/IUser";
import {IUserJSON} from "../models/IUserJSON";

@Injectable({
  providedIn: 'root'
})
export class UserService {

  constructor(private httpClient: HttpClient) {
  }

  doSomeStuff() {
    console.log('metod of user servises')
  }

  getUsers(): Observable<IUserJSON[]> {
    return  this.httpClient.get<IUserJSON[]>('https://jsonplaceholder.typicode.com/users')
  }
}