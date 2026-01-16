import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class NewsDataService {
  private apiKey = '601538b3642945e69c7e30cf879cbdd4';
  private baseUrl = 'https://newsapi.org/v2/';

  constructor(private http: HttpClient) { }

  getLatestAmericanSportsNews(): Observable<any> {
    const url = `${this.baseUrl}top-headlines?category=sports&country=us&apiKey=${this.apiKey}`;
    return this.http.get(url);
  }
}