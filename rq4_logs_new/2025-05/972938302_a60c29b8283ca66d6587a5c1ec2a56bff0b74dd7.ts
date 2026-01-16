import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from '../../../environments/environments';
import { Observable } from 'rxjs';
import { AlunoResponse } from '../interface/alunoResponse';

@Injectable({
  providedIn: 'root'
})
export class AlunoService {
  private url = `${environment.api_url}/alunos`

  constructor(private http: HttpClient) { }

  getAlunos(): Observable<AlunoResponse[]> {
    return this.http.get<AlunoResponse[]>(`${this.url}`);
  }
}