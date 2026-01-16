import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse, HttpHeaders } from '@angular/common/http';
import { catchError, Observable, retry, throwError } from 'rxjs';
import { Conference } from 'src/app/model/Conference';
import { ProgrammeDetails } from 'src/app/model/ProgrammDetails';
import { Programms } from 'src/app/model/Programms';
import { Speakers } from 'src/app/model/Speakers';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class ApiServiceService {
  httpOption: any;
  constructor(private http: HttpClient) {
    this.httpOption = {
      headers: new HttpHeaders({
        'Content-Type': 'application/json',
        // ,Authorization: 'my-auth-token'
      }),
    };
  }
 
  getconference(): Observable<Conference[]> {
    return this.http.get<Conference[]>(`${environment.APIURL}conferences`);
  }
  getconferenceById(confID: number): Observable<Conference> {
    return this.http.get<Conference>(`${environment.APIURL}conferences/${confID}`);
  }
  getconferenceSpeakers(confID: number): Observable<Conference> {
    return this.http.get<Conference>(`${environment.APIURL}conferences/${confID}/speakers`);
  }
  getconferenceProgrammes(confID: number): Observable<Conference> {
    return this.http.get<Conference>(`${environment.APIURL}conferences/${confID}/programmes`);
  }
  getconferenceAbout(confID: number): Observable<Conference> {
    return this.http.get<Conference>(`${environment.APIURL}conferences/6542b68e478df56679e32f22/about`);
  }

  //programme details

  getprogrammedetails (progID: number): Observable<ProgrammeDetails> {
    return this.http.get<ProgrammeDetails>(`${environment.APIURL}details/${progID}`);
  }

  //programmes
  getprogrammes (): Observable<Programms> {
    return this.http.get<Programms>(`${environment.APIURL}programmes`);
  }
  getprogrammesById (progID: number): Observable<Programms> {
    return this.http.get<Programms>(`${environment.APIURL}programmes/${progID}`);
  }
  getprogrammesDetails (progID: number): Observable<Programms> {
    return this.http.get<Programms>(`${environment.APIURL}programmes/${progID}/details`);
  }

  //speakers
  getspeakers (): Observable<Speakers> {
    return this.http.get<Speakers>(`${environment.APIURL}speakers`);
  }
}