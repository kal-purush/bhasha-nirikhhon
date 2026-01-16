import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';
import { FriendFollower } from '../models/friendfollower';

const httpOptions={
  headers: new HttpHeaders({
    'content-type': 'application/json'
  })
}

@Injectable({
  providedIn: 'root'
})
export class FriendfollowerService {

  baseApiUrl: string = environment.baseApiUrl;
  constructor(private http: HttpClient) { }

  getUserFriends(id: number): Observable<FriendFollower[]>{
    return this.http.get<FriendFollower[]>(this.baseApiUrl + 'friendfollowers/' + id + '/friends');
  }

  getUserFollowers(id: number): Observable<FriendFollower[]>{
    return this.http.get<FriendFollower[]>(this.baseApiUrl + 'friendfollowers/' + id + '/followers');
  }

  followUser(targetId: number): Observable<FriendFollower>{
    let friendFollower = new FriendFollower;
    friendFollower.userId = Number(localStorage.getItem('userid'));
    friendFollower.otherUserId = targetId;
    return this.http.post<FriendFollower>(this.baseApiUrl + 'friendfollowers/follow', friendFollower, httpOptions);
  }

  unfollowUser(targetId: number): Observable<boolean>{
    let userId =  Number(localStorage.getItem('userid'));
    return this.http.delete<boolean>(this.baseApiUrl + 'friendfollowers/' + userId + '/unfollow/' + targetId, httpOptions);
  }

  blockUser(targetId: number): Observable<FriendFollower>{
    let userId =  Number(localStorage.getItem('userid'));
    return this.http.put<FriendFollower>(this.baseApiUrl + 'friendfollowers/' + userId + '/block/' + targetId, httpOptions);
  }

  unblockUser(targetId: number): Observable<boolean>{
    let userId = Number(localStorage.getItem('userid'));
    return this.http.delete<boolean>(this.baseApiUrl + 'friendfollowers/' + userId + '/unblock/' + targetId, httpOptions);
  }

  unfriendUser(targetId: number): Observable<boolean>{
    let userId = Number(localStorage.getItem('userid'));
    return this.http.delete<boolean>(this.baseApiUrl + 'friendfollowers/' + userId + '/unfriend/' + targetId, httpOptions);
  }

}