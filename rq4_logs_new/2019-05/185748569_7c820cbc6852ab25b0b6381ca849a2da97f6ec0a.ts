import { ResourceResponse } from './resourceResponse';
import { Resource } from './resource';

import { HttpClient, HttpResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ResourceService {

  rootUrl: string = 'http://virtserver.swaggerhub.com/szutsj/godess-tribes-backend/1.0.0';

  constructor(private http: HttpClient) { }

  getDataFromBackend(): Observable<ResourceResponse> {
    return this.http.get<ResourceResponse>(this.rootUrl + '/kingdom/resources');
  }
  
}