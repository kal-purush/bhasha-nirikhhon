import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { Category } from '../../user/shared/types';

@Injectable({ providedIn: 'root' })
export class CategoriesService {

  ROOT_URL = 'http://52.57.253.240:3000/api/categories';

  constructor(private http: HttpClient) {}

  getCategories(): Observable<Category[]> {
    const httpOptions = {
      headers: new HttpHeaders({
       'Content-Type':  'application/json',
        Authorization: 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjo0fQ.5rkd4zXemzKWHF9i-VylcjH_ll306bU7wDVrFk52BRA'
      })
    };

    return this.http.get<Category[]>(this.ROOT_URL, httpOptions).pipe(
      map((response) => response)
    );
  }

}