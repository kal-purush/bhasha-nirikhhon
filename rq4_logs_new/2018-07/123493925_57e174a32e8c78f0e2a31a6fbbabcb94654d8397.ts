import { Injectable } from '@angular/core';
import { Subject } from "rxjs/Subject";
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { TokenManagerService } from './token-manager.service';

@Injectable()
export class EventsApiService {

constructor(private http: HttpClient, private tokenManagerService: TokenManagerService) { }
postEvent(event) {
  let token = this.tokenManagerService.retrieveToken();
  return this.http.post("http://millennialsvote.azurewebsites.net/api/activities", event, {headers: new HttpHeaders().set('Authorization', 'Bearer ' + token)});
}



}