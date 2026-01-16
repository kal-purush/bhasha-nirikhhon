import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import { Observable } from 'rxjs/Observable';

export class Alert {
  type: AlertType;
  message: string;
  constructor(type: AlertType, message: string) {
    this.type = type;
    this.message = message;
  }
}

export enum AlertType {
  Success,
  Error
}

@Injectable()
export class StatusMessageService {

  constructor() { }

  private alert: BehaviorSubject<Alert> = new BehaviorSubject<Alert>(null);
  public currentAlert: Observable<Alert> = this.alert.asObservable();

  success(message: string) {
    const success = new Alert(AlertType.Success, message);
    this.alert.next(success);
  }

  error(message: string) {
    const success = new Alert(AlertType.Error, message);
    this.alert.next(success);
  }

  clearAlert() {
    this.alert.next(null);
  }

}