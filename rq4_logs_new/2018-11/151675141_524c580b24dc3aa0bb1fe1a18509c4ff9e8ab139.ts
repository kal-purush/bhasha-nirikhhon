import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';
import { Alert, AlertType } from './alert';

@Injectable({
  providedIn: 'root'
})
export class AlertService {

  alertSubject: Subject<Alert> = new Subject<Alert>();

  private alert(alertType: AlertType, message: string) {

    this.alertSubject.next(new Alert(alertType, message));
  }

  success(message: string) {
    this.alert(AlertType.SUCCESS, message);
  }

  warning(message: string) {
    this.alert(AlertType.WARNING, message);
  }

  danger(message: string) {
    this.alert(AlertType.DANGER, message);
  }

  info(message: string) {
    this.alert(AlertType.INFO, message);
  }

  getAlert() {
    return this.alertSubject.asObservable();
  }
}