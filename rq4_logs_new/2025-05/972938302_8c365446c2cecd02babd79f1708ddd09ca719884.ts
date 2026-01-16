import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environments';
import { Observable } from 'rxjs';
import { DadosGraphProfListResponse } from '../types/dados-graph-prof-list.type';
import { DadosGraphNoteListResponse } from '../types/dados-graph-note-list-response.type';

@Injectable({
  providedIn: 'root'
})
export class GraphNoteService {
  private url = `${environment.api_url}/dashboard/grafico-nota`

  constructor(private http: HttpClient) { }

  getDataGraph(): Observable<DadosGraphNoteListResponse> {
    return this.http.get<DadosGraphNoteListResponse>(`${this.url}`);
  }
}