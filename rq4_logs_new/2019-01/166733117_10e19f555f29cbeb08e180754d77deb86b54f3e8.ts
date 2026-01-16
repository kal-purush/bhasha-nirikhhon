import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class MidwareService {

  constructor(private http: HttpClient) { }

  checkGet(){
    //return this.http.get('https://reqres.in/api/users');
    return this.http.get('http://localhost:8080');
  }

  checkPost(){
    return this.http.post('http://localhost:8080', 'any');
  }

  checkPut(){
    return this.http.put('http://localhost:8080', 'any');
  }

  checkDelete(){
    return this.http.delete('http://localhost:8080');
  }
}