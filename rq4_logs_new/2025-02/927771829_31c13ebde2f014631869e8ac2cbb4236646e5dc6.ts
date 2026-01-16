import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { Thinking } from '../interfaces/thinking';
import { environment } from '../../environments/environment';

@Injectable({
  providedIn: 'root'
})
export class ThinkingService {
  private readonly API = environment.apiUrl
  constructor(private readonly http: HttpClient) { }
  list(): Observable<Thinking[]> {
    return this.http.get<Thinking[]>(this.API)
  }

  create(thinking: Thinking): Observable<Thinking> {
    return this.http.post<Thinking>(this.API, thinking)
  }
  edit(thinking: Thinking): Observable<Thinking> {
    return this.http.put<Thinking>(`${this.API}/${thinking.id}`, thinking)
  }
  delete(id: number): Observable<Thinking> {
    return this.http.delete<Thinking>(`${this.API}/${id}`)
  }
  findById(id: number): Observable<Thinking> {
    return this.http.get<Thinking>(`${this.API}/${id}`)
  }
}