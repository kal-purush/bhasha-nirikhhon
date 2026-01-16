import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { map, Observable } from 'rxjs';
import { IComments } from '../models/icomments';

@Injectable({
  providedIn: 'root'
})
export class CommentsService {

  private apiUrl = 'http://localhost:3000/comments';

  constructor(private http: HttpClient) {}
  private token: string = localStorage.getItem('token') || '';



  addComment(_id: string, content: string): Observable<IComments> {
    const body = { id_post: _id, content };
    return this.http.post<IComments>(`${this.apiUrl}/add`, body);
  }
  

  showComment(){}

}