import { Injectable } from '@angular/core';
import { Ingredients } from './ingredients';
import { Observable, of } from 'rxjs';
import { catchError, tap } from 'rxjs/operators';
import { HttpClient, HttpHeaders } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})

export class IngredientsService {

  httpOptions = {
    headers: new HttpHeaders({ 'Content-Type': 'application/json' })
  };

  constructor(private http: HttpClient) { }

  addIngredients(ingredients: Ingredients): Observable<any> {
    return this.http.post<Ingredients>('http://localhost:4000/api/create', ingredients, this.httpOptions)
    .pipe(
      catchError(this.handleError<Ingredients>('Add Ingredients'))
    ); 
  }
  
  getIngredients(id):Observable<Ingredients[]>{
    return this.http.get<Ingredients[]>('http://localhost:4000/api/ingredients'+ id)
    .pipe(
      tap(_ => console.log(`Ingredients found: ${id}`)),
      catchError(this.handleError<Ingredients[]>('get all ingredients'))
    );
  }
  
  getIngredientsList(): Observable<Ingredients[]> {
    return this.http.get<Ingredients[]>('http://localhost:4000/api')
     .pipe(
      tap(ingredients => console.log('All Ingredient Lists fetched!')),
      catchError(this.handleError<Ingredients[]>('Get all Ingredients', []))
    );
  }

  updateIngredients(id, ingredients: Ingredients) : Observable<any> {
    return this.http.put('http://localhost:4000/api/update-ingredientsList/' + id, ingredients, this.httpOptions)
      .pipe(
        tap(_ => console.log(`Ingredients updated: ${id}`)),
        catchError(this.handleError<Ingredients[]>('Update Ingredients'))
      );
  }

  deleteIngredients(id): Observable<Ingredients[]> {
    return this.http.delete<Ingredients[]>('http://localhost:4000/api/delete-ingredients/' + id, this.httpOptions)
      .pipe(
        tap(_ => console.log(`Ingredient List deleted: ${id}`)),
        catchError(this.handleError<Ingredients[]>('Delete Ingredient List'))
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