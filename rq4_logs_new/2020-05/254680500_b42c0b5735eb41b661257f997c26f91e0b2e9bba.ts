import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { Setting } from '../_models/setting';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class SettingsService {

  private baseUrl = `${environment.apiUrl}settings`;

  httpOptions = {
    headers: new HttpHeaders({
      'Content-Type': 'application/json'
    })
  };

  constructor(private http: HttpClient) {
  }

  getSettings(): Observable<Setting[]>{
    return this.http.get<Setting[]>(this.baseUrl)
      .pipe(map(data => data.map(x => {
        return new Setting().deserialize(x);
      })));
  }

  saveSettings(settings){
    return this.http.post(this.baseUrl, JSON.stringify(settings), this.httpOptions);
  }
}