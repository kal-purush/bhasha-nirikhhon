import { KingdomNameResponse } from './kingdomNameResponse';
import { Injectable } from '@angular/core';
import { HttpClient, HttpResponse } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class HeaderService {

  rootUrl = 'http://virtserver.swaggerhub.com/szutsj/godess-tribes-backend/1.0.0';

  constructor(private http: HttpClient) { }

  getDataFromBackend(): Observable<KingdomNameResponse> {
    return this.http.get<KingdomNameResponse>(this.rootUrl + '/kingdom');
  }
}