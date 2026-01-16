import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environments';
import { Observable } from 'rxjs';
import { AlunoResponse } from '../interface/alunoResponse';
import { AlunoListResponse } from '../types/aluno-list-response.type';
import { CardDashboardListResponse } from '../types/card-dashboard-list-response.type';

@Injectable({
  providedIn: 'root'
})
export class CardDashboardService {
  private url = `${environment.api_url}/dashboard/cards`

  constructor(private http: HttpClient) { }

  getCardsDashboard(): Observable<CardDashboardListResponse> {
    return this.http.get<CardDashboardListResponse>(`${this.url}`);
  }
}