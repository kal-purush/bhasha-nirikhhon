import { Injectable } from '@angular/core';
import { Calories } from './calories';
import { Observable, of } from 'rxjs';
import { catchError, tap } from 'rxjs/operators';
import { HttpClient, HttpHeaders } from '@angular/common/http';

@Injectable({
    providedIn: 'root'
  })

export class CaloriesService {
    
    httpOptions = {
        headers: new HttpHeaders({ 'Content-Type': 'application/json' })
      };

    constructor(private http: HttpClient) { }

    addCalories(calories: Calories): Observable<any> {
        return this.http.post<Calories>('http://localhost:4000/api/calories/create', calories, this.httpOptions)
        .pipe(
          catchError(this.handleError<Calories>('Add Calories'))
        ); 
      }
      
      getCalories(id):Observable<Calories>{
        return this.http.get<Calories>('http://localhost:4000/api/calories'+ id)
        .pipe(
          tap(_ => console.log(`Calories found: ${id}`)),
          catchError(this.handleError<Calories>('get all calories'))
        );
      }
      
      getCaloriesList(): Observable<Calories[]> {
        return this.http.get<Calories[]>('http://localhost:4000/api')
         .pipe(
          tap(ingredients => console.log('All Ingredient Lists fetched!')),
          catchError(this.handleError<Calories[]>('Get all calories'))
        );
      }
    
      updateCalories(id, calories: Calories) : Observable<any> {
        return this.http.put('http://localhost:4000/api/update-calories/' + id, calories, this.httpOptions)
          .pipe(
            tap(_ => console.log(`Calories updated: ${id}`)),
            catchError(this.handleError<Calories[]>('Update Calories'))
          );
      }
    
      deleteCalories(id): Observable<Calories> {
        return this.http.delete<Calories>('http://localhost:4000/api/delete-calories/' + id, this.httpOptions)
          .pipe(
            tap(_ => console.log(`Calories deleted: ${id}`)),
            catchError(this.handleError<Calories>('Delete Calories'))
          );
      }
    
      private handleError<T>(operation = 'operation', result?: T) {
        return (error: any): Observable<T> => {
          console.error(error);
          console.log(`${operation} failed: ${error.message}`);
          return of(result as T);
        };
      }
}