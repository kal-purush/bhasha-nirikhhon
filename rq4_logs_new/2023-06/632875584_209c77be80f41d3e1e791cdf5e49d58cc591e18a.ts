import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';
import { FriendRequest } from '../models/friendrequest';

const httpOptions={
  headers: new HttpHeaders({
    'content-type': 'application/json'
  })
}

@Injectable({
  providedIn: 'root'
})
export class FriendrequestService {

  baseApiUrl: string = environment.baseApiUrl;
  constructor(private http: HttpClient) { }

  getReceivedRequests(id: number): Observable<FriendRequest[]>{
    return this.http.get<FriendRequest[]>(this.baseApiUrl + 'friendrequests/received/' + id);
  }

  getSentRequests(id: number): Observable<FriendRequest[]>{
    return this.http.get<FriendRequest[]>(this.baseApiUrl + 'friendrequests/sent/' + id);
  }

  sendFriendRequest(request: FriendRequest): Observable<FriendRequest>{
    return this.http.post<FriendRequest>(this.baseApiUrl + 'friendrequests', request, httpOptions);
  }
}