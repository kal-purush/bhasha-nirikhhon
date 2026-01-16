import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { IProfile } from '../types/dashboard-state/dashboard-state.interface';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { ILanguage } from './../types/dashboard-state/dashboard-state.interface';

@Injectable()
export class DashboardService {
  private baseUrl = 'http://localhost:3000/';
  constructor(private http: HttpClient) {}

  getProfile() {
    return this.http.get<IProfile>(this.baseUrl + 'profile');
  }
  getLanguages() {
    return this.http.get<ILanguage>(this.baseUrl + 'languages');
  }
}