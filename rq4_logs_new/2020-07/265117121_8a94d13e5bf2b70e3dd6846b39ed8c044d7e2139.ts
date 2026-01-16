import { Injectable } from '@angular/core';
import { HttpHeaders, HttpClient } from '@angular/common/http';
import { environment } from '../../../environments/environment';
import { Observable, of } from 'rxjs';
import { Article } from '../article/article';
import { catchError, tap } from 'rxjs/operators';

const httpOptions = {
  headers: new HttpHeaders({'Content-Type': 'application/json'})
};
const apiUrl = environment.production ? '/api/articles/' : 'http://localhost:3000/api/articles';

@Injectable({
  providedIn: 'root'
})
export class BagService {
  [x: string]: any;

  constructor(private httpClient: HttpClient) { }
  getAll(): Observable<Article[]> {
    return this.httpClient.get<Article[]>(apiUrl)
    .pipe(
      catchError(this.handleError('getAll', []))
    );
  }

  addArticle(article: Article): Observable<Article> {
    return this.httpClient.post<Article>(apiUrl, article, httpOptions).pipe(
      tap((a: Article) => console.log(`added article w/ id=${a._id}`)),
      catchError(this.handleError<Article>('addArticle'))
    );
  }
  updateArticle(id: string, article: Article): Observable<any> {
    const url = `${apiUrl}/${id}`;
    return this.httpClient.put(url, article, httpOptions).pipe(
      tap(_ => console.log(`updated article id=${id}`)),
      catchError(this.handleError<any>('updateArticle'))
    );
  }
  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {

    // TODO: send the error to remote logging infrastructure
    console.error(error); // log to console instead

    // Let the app keep running by returning an empty result.
    return of(result as T);
    };
  }
}