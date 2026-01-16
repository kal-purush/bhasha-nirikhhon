
import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Proposal } from '../types';

@Injectable({ providedIn: 'root' })
export class NeedService {
  ROOT_URL = 'http://52.57.253.240:3000/api/proposals';

  constructor(private http: HttpClient) { }

  postData(data: Proposal): Observable<any> {
    // const token = localStorage.getItem('token');

    const httpOptions = {
      headers: new HttpHeaders({
        'Content-Type':  'application/json',
        Authorization: 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjo0fQ.5rkd4zXemzKWHF9i-VylcjH_ll306bU7wDVrFk52BRA'
        // Authorization: `Bearer ${token}`
      })
    };

    const body = JSON.stringify(data);

    return this.http.post<Proposal>(this.ROOT_URL, body, httpOptions);
  }

}