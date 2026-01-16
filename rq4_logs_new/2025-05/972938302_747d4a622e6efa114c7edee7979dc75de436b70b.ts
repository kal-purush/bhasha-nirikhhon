import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environments';
import { Observable } from 'rxjs';
import { DadosGraphProfListResponse } from '../types/dados-graph-prof-list.type';

@Injectable({
  providedIn: 'root'
})
export class GraphProfService {
  private url = `${environment.api_url}/status-disciplinas`

  constructor(private http: HttpClient) { }

  getDataGraph(): Observable<DadosGraphProfListResponse> {
    return this.http.get<DadosGraphProfListResponse>(`${this.url}`);
  }
}