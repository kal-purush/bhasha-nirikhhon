import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class UserServiceService {


  public API: string = 'http://localhost:8433/api/v1/users';

  constructor(public http: HttpClient) {
  }

  getUserEdit(id:number):Observable<any>{
    return this.http.get(`${this.API}/${id}`);
  }


  updateUser(user):Observable<any>{
    return this.http.put(`${this.API}/${user.idUser}`,user)
  }
}
