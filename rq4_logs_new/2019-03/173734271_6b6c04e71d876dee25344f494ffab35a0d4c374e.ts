import { Injectable } from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {Observable} from 'rxjs';
import {Job} from '../../models/job';

@Injectable({
  providedIn: 'root'
})
export class JobService {

  constructor(private http: HttpClient ) {
  }
  getJobs(): Observable<Job[]> {
    return this.http.get<Job[]>(`https://calm-waters-93672.herokuapp.com/job`);
  }

  createAuthorizationHeader(headers: HttpHeaders) {
    headers.append('Content-Type', 'application/json');
  }

  addJob(job: Job) {
    const headers = new HttpHeaders();
    this.createAuthorizationHeader(headers);
    return this.http.post(`https://calm-waters-93672.herokuapp.com/job`);
  }
}