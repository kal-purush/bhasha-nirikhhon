import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpErrorResponse } from "@angular/common/http";
import { Observable, of, throwError } from 'rxjs';
import { awsUrl, localUrl } from 'src/environments/environment';
import { catchError, map } from "rxjs/operators";
import { Review } from '../models/review';
import { Movie } from '../models/movie';
import { User } from '../models/user';
import { Follow } from '../models/follow';

const url = localUrl + '/follows';

@Injectable({
  providedIn: 'root'
})
export class FollowService {

  constructor(private http: HttpClient) { }
  httpOptions = {
    headers: new HttpHeaders({ 'Content-Type': 'application/json' })
  }


  public addFollow(userFollower: User, userToFollow: User): Observable<Follow> {
    // Do this to avoid "object references an unsaved transient instance"
    userFollower = new User(userFollower.id, '', '', '', '', '', [], [], []);
    userToFollow = new User(userToFollow.id, '', '', '', '', '', [], [], []);
    let follow: Follow = new Follow(0, userFollower, userToFollow, new Date())
    return this.http.post<Follow>(`${url}/add`, follow, this.httpOptions).pipe(catchError(this.handleError))
  }

  public removeFollow(followId: number) {
    // Do this to avoid "object references an unsaved transient instance"
    return this.http.delete(`${url}/${followId}`, this.httpOptions).pipe(catchError(this.handleError))
  }

  // Removes a follow from a given Following list
  public removeFromStorage(following: Follow[], idToRemove: number): Follow[] {
    following = following = following.filter(follow => {
      return follow.followId !== idToRemove;
    })

    return following;
  }

  private handleError(httpError: HttpErrorResponse) {
    if (httpError instanceof ErrorEvent) {
      console.log('And error occurred: ', httpError);
    }
    else {
      console.error(`
        Backend returned code ${httpError.status},
        body was: ${httpError.error}
      `)
    }
    return throwError('Something bad happened; please try again later');
  }
}