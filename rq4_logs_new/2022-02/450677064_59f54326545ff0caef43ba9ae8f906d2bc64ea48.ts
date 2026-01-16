import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'environments/environment';
import Reponse from '../../interfaces/Reponse';

@Injectable({
  providedIn: 'root'
})
export class ReponseService {

  private baseUrl: string = environment.apiUrl + '/Reponses';

  constructor(private http: HttpClient) { }


  get(id: string): Observable<any> {
    return this.http.get(`${this.baseUrl}/${id}`);
  }

  update(id: string, reponse: Reponse): Observable<Reponse> {
    return this.http.put<Reponse>(`${this.baseUrl}/${id}`, reponse);
  }

  delete(id:string): Observable<Reponse> {
    return this.http.delete<Reponse>(`${this.baseUrl}/${id}`);
  }
}