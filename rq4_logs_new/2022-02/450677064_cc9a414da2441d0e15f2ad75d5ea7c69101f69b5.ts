import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'environments/environment';
import Question from '../../interfaces/Question';

@Injectable({
  providedIn: 'root'
})
export class QuestionService {

  private baseUrl: string = environment.apiUrl + '/Questions';

  private corsHeader = new HttpHeaders({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS,DELETE,PUT'
  });

  constructor(private http: HttpClient) {}

  get(id: string): Observable<Question> {
    return this.http.get<Question>(`${this.baseUrl}/${id}`, {headers: this.corsHeader});
  }

  update(id: string, question: Question): Observable<Question> {
    return this.http.put<Question>(`${this.baseUrl}/${id}`, question);
  }

  delete(id: string): Observable<Question> {
    return this.http.delete<Question>(`${this.baseUrl}/${id}`);
  }

}