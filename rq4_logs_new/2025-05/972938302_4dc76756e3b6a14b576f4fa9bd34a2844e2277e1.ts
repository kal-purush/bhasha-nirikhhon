import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environments';
import { Observable } from 'rxjs';
import { CardDashboardListResponse } from '../types/card-dashboard-list-response.type';

@Injectable({
  providedIn: 'root'
})
export class CardDashboardAlunService {
  private url = `${environment.api_url}/dashboard/visao-alunos`

  constructor(private http: HttpClient) { }

  getCardsDashboard(): Observable<CardDashboardListResponse> {
    return this.http.get<CardDashboardListResponse>(`${this.url}`);
  }
}