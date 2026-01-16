import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';
import { HttpClient } from '@angular/common/http';
import { Application } from '../models/application.model';

@Injectable({
  providedIn: 'root'
})
export class ApplicationService {

  private applicationUrl =  `${environment.apiBaseUrl + '/applications'}`;
  constructor(private http: HttpClient) { }

  getaplication (id: string) {
   
    const url = `${this.applicationUrl}/${id}`;
    return this.http.get<Application>(url).toPromise();

  }

  
  getApplications () {
   
    
    return this.http.get<Application[]>(this.applicationUrl).toPromise();

  }
}