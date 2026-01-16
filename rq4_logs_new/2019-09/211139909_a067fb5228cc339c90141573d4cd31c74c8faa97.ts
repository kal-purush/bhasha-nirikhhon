import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, Subject, of } from 'rxjs';
import { map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})

export class EditService {
  users: User[] = [];
  username: 

 private usersEndpoint: string = 'http://localhost:3000/users/update/:id';
 private httpOptions = {
   headers: new HttpHeaders({
     'Content-Type':  'application/json'
   }),

   withCredentials:true
 };




  constructor(private http: HttpClient) { }

  getUser(userId: number){
    return this.http.get(`$this.usersEndpoint}${username} ${email}`, this.httpOptions)
    .pipe(map(res=> <any[]>res));
  }

  editUser(userId: number){
    return this.http.put(`${this.usersEndpoint}${email}`, this.httpOptions)
    .pipe(map(res => <any[]>res));
  }

  deleteUser(userId: number) {
    return this.http.delete(`${this.usersEndpoint}${userId}`, this.httpOptions)
      .pipe(map(res => <any[]>res));
  }
}