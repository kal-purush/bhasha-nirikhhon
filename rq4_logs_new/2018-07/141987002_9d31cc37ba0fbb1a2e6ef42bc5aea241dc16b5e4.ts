import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Router } from '@angular/router';
import { HttpModule } from '@angular/http';


@Injectable({
  providedIn: 'root'
})
export class UserServiceService {

  constructor(private http: HttpClient, private router: Router) { }

  registerUser(req): Observable<any> {
    return this.http.post<any>('http://localhost:8000/api/users/', req);
  }

}