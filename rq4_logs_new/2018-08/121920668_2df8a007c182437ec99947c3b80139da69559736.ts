import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { catchError, map, tap, timeout } from 'rxjs/operators';

import { environment } from '../environments/environment';
import { PlannerImage } from './image';

const httpOptions = {
  headers: new HttpHeaders({
    'Content-Type': 'application/json'
  })
};

@Injectable({
  providedIn: 'root'
})
export class ImageService {

  private rootUrl = environment.plannerBackendRootUrl;
  private apiContext = environment.plannerBackendImageContext;

  constructor(private http: HttpClient) { }

  uploadImages(images: FormData): Observable<PlannerImage[]> {
    return this.http.post<FormData>(this.rootUrl + this.apiContext, images, httpOptions)
      .pipe(
        catchError(this.handleError('uploadImages', null))
      );
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      console.error(error);
      alert(`${operation} failed - check the console for more information`);
      return of(result as T);
    }
  }
}