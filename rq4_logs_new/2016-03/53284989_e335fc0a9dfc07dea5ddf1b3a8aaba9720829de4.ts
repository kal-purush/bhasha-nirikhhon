import {Injectable} from 'angular2/core';
import {Comment} from './comment'
import {Http, Headers} from 'angular2/http';
import {Observable}     from 'rxjs/Observable';
import 'rxjs/Rx';

@Injectable()
export class CommentService {
  constructor(private http: Http) {
  }

  getCommentsObservable(): Observable<Comment[]> {
    return this.http.get('/api/comments').map(res => res.json() as Comment[])
                                         //.delay(2000)
                                         //.repeat()
  }

  saveCommentObservable(comment: Comment): Observable<Comment[]> {
    const headers = new Headers();
    headers.append('Content-Type', 'application/json');
    return this.http.post('/api/comments', JSON.stringify(comment), {headers:headers})
      .map(res => res.json() as Comment[])
  }
}