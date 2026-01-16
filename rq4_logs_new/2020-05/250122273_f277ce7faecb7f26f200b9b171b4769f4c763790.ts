import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { SERVER_API_URL } from 'app/app.constants';
import { Observable } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class TopNavBarService {
  constructor(private http: HttpClient) {}

  revisionsCount(): Observable<{}> {
    return this.http.get(SERVER_API_URL + 'services/icamapi/api/revisions/count');
  }

  articlesCount(): Observable<{}> {
    return this.http.get(SERVER_API_URL + '/services/icamapi/api/articles/count');
  }
}