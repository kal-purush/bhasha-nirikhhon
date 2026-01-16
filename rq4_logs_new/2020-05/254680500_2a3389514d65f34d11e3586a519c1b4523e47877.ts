import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';
import { Observable } from 'rxjs';
import { Profile } from '../_models/profile';
import { HandleErrorsService } from './handle-errors.service';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { catchError } from 'rxjs/internal/operators/catchError';

@Injectable({
  providedIn: 'root'
})
export class FriendsService {

  profilesUrl = `${environment.apiUrl}profiles/`;

  httpOptions = {
    headers: new HttpHeaders({
      'Content-Type': 'application/json'
    })
  };


  constructor(
    private http: HttpClient,
    private errorHandler: HandleErrorsService) {
  }


  public sendFriendRequest(targetId: string, toInvite: string): Observable<Profile> {
    return this.http.post<Profile>(this.profilesUrl + 'friends/invite',
      null, { headers: this.httpOptions.headers, params: { targetId, toInvite } }
    ).pipe(catchError(this.errorHandler.handleError<any>('sendFriendRequest')));
  }

  public processFriendRequest(targetId: string, toAccept: string): Observable<Profile> {
    return this.http.post<Profile>(this.profilesUrl + 'friends/process',
      null, { headers: this.httpOptions.headers, params: { targetId, toAccept } }
    ).pipe(catchError(this.errorHandler.handleError<any>('processFriendRequest')));
  }


  public removeFriend(targetId: string): Observable<Profile> {
    return this.http.post<Profile>(this.profilesUrl + 'friends/remove',
      null, { headers: this.httpOptions.headers, params: { targetId } }
    ).pipe(catchError(this.errorHandler.handleError<any>('removeFriend')));
  }


  public getUsersFriends(targetId: string, page: string): Observable<Profile[]> {
    return this.http.get<Profile[]>(this.profilesUrl + targetId + `/friends/page/` + page,
      this.httpOptions).pipe();
  }


  public getUsersFriendsSize(targetId: string): Observable<number> {
    return this.http.get<number>(this.profilesUrl + targetId + `/friendstotalsize`,
      this.httpOptions).pipe();
  }


  public getUsersInvitationsSize(direction: string): Observable<number> {
    if (direction === 'outgoing' || direction === 'incoming') {
      return this.http.get<number>(this.profilesUrl + 'friends/invitations' + direction + 'totalsize',
        this.httpOptions).pipe();
    }
  }


}