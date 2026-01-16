import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class AuthService {

  constructor(private http:HttpClient) { }

  register(data:any):Observable<any>{
    return this.http.post('http://localhost:4000/auth/register', data)
  }

  login(data:any):Observable<any>{
    
    return this.http.post('http://localhost:4000/auth/SignIn', data)
  }

  profile():Observable<any>{
    let headers={
      'Authorization':'Bearer '+ localStorage.getItem('token')
    }
    return this.http.get('http://localhost:4000/auth/profile', {headers:headers})
  }

}