import { HttpErrorResponse } from '@angular/common/http';
import { Firestore, collectionData, collection, doc } from '@angular/fire/firestore';
import { NEVER, Observable, throwError } from 'rxjs';
import { catchError, map, tap } from 'rxjs';
export abstract class FireBaseFacade<T> {
  constructor(private readonly firestore: Firestore, private collection: string) {}

  getAll() {
    const ref = collection(this.firestore, this.collection);
    return collectionData(ref, { idField: 'id' }).pipe(
      map((object) => object as T[]),
      catchError(this.handleError)
    );
  }

  handleError(err: HttpErrorResponse): Observable<never> {
    let errorMessage: string;
    if (err.error instanceof ErrorEvent) {
      errorMessage = `An error occurred: ${err.error.message}`;
    } else {
      errorMessage = `Backend returned code ${err.status}: ${err.message}`;
    }
    console.error(err);
    return throwError(() => errorMessage);
  }
}