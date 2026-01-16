import { Injectable } from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {Observable} from 'rxjs';
import { Comment } from '../comment';

@Injectable({
  providedIn: 'root'
})
export class CommentsService {

  private commentsUrl = 'http://localhost:8080/storage/comments/allcomments';

  httpOptions = {
    headers: new HttpHeaders({ 'Content-Type': 'application/json' })
  };

  constructor(private http: HttpClient) { }

  /** GET comments from the server */
  getComments(): Observable<Comment[]> {
    return this.http.get<Comment[]>(this.commentsUrl);
  }

  /** POST: add a new comment to the server */
  // addComment(comment: Comment): Observable<Comment> {
  //   return this.http.post<Comment>(this.commentsUrl, comment, this.httpOptions);
  // }

  /** DELETE: delete the video from the server */
  // deleteVideo(video: Video | number): Observable<Video> {
  //   const id = typeof video === 'number' ? video : video.id;
  //   const url = `${this.videosUrl}/${id}`;
  //
  //   return this.http.delete<Video>(url, this.httpOptions);
  // }

  /** PUT: update the video on the server */
  // updateVideo(video: Video): Observable<any> {
  //   return this.http.put(this.videosUrl, video, this.httpOptions);
  // }
}