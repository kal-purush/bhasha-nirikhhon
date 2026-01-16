import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
declare let alertify: any;

@Injectable({
  providedIn: 'root'
})
export class AlertifyService {

  constructor() { }

  success(message: string) {
    alertify.success(message);
  }

  error(message: string) {
    alertify.error(message);
  }

  warning(message: string) {
    alertify.warning(message);
  }

  message(message: string) {
    alertify.message(message);
  }

  confirm(message: string): Observable<boolean> {
    return Observable.create(observer => alertify.confirm('Swarmer', message, () => observer.complete(true), () => observer.complete(false)).set('transition','zoom'));
  }
}