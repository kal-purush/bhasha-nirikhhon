import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';

import { config } from '@/config';

const httpOptions = {
  headers: new HttpHeaders({
    'Content-Type':  'application/json'
    })};

@Injectable({
  providedIn: 'root'
})
export class AssignmentUserService {

  constructor(private http: HttpClient) { }

  getAll(user, course): Observable<any> {
      return this.http.get(`${config.apiUrl}/assignments-user/${user}/${course}`);
  }

}