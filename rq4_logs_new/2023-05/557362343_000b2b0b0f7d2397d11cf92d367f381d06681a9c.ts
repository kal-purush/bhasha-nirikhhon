import { Injectable } from '@angular/core';
import {
  AbstractControl,
  AsyncValidator,
  ValidationErrors,
} from '@angular/forms';
import { Observable } from 'rxjs';
import { delay } from 'rxjs/operators';

@Injectable({ providedIn: 'root' })
export class EmailValidator implements AsyncValidator {
  // validate(control: AbstractControl): Observable<ValidationErrors | null> {
  //   const email = control.value;
  //   console.log({ email });
  //   return of({
  //     emailTaken: true,
  //   }).pipe(delay(2000));
  // }

  validate(control: AbstractControl): Observable<ValidationErrors | null> {
    const email = control.value;
    console.log({ email });

    const httpCallObservable = new Observable<ValidationErrors | null>(
      (subscriber) => {
        console.log({ email });
        if (email === 'miguelangel@gmail.com') {
          subscriber.next({ emailTaken: true });
          subscriber.complete();
          // return;
        }
        subscriber.next(null);
        subscriber.complete();
      }
    ).pipe(delay(2000));

    return httpCallObservable;
  }
}