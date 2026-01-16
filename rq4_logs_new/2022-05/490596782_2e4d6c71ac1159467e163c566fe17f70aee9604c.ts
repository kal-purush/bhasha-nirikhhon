import { HttpErrorResponse } from '@angular/common/http';
import {
  Firestore,
  collectionData,
  collection,
  doc,
  docData,
  deleteDoc,
  addDoc,
  updateDoc,
} from '@angular/fire/firestore';

import { NEVER, Observable, throwError } from 'rxjs';
import { catchError, map, tap } from 'rxjs';
export abstract class FireBaseFacade<T> {
  constructor(private readonly firestore: Firestore, private collection: string) {}

  getAll(): Observable<T[]> {
    const ref = collection(this.firestore, this.collection);
    return collectionData(ref, { idField: 'id' }).pipe(
      tap((data) => {
        console.log(data);
      }),
      map((object) => object as T[]),
      catchError(this.handleError)
    );
  }

  get(id: String): Observable<T> {
    const ref = doc(this.firestore, `${this.collection}/${id}`);
    return docData(ref, { idField: 'id' }).pipe(
      map((object) => object as T),
      catchError(this.handleError)
    );
  }

  async add(data: T) {
    const ref = collection(this.firestore, this.collection);
    const docRef = await addDoc(ref, data);
    return docData(docRef, { idField: 'id' }).pipe(
      map((object) => object as T),
      catchError(this.handleError)
    );
  }

  async delete(id: string) {
    return deleteDoc(doc(this.firestore, `${this.collection}/${id}`));
  }

  async update(id: string, data: T) {
    updateDoc(doc(this.firestore, `${this.collection}/${id}`), data);
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