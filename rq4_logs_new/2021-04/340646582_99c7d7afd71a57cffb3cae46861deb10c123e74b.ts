import {Injectable} from "@angular/core";
import {Notification} from "../model/notification.model";

@Injectable({providedIn: 'root'})
export class NotificationService {

  constructor() {}

  public notifications: Notification[] = [];
  public count: number = 0;

  public add(action: string, id?: number): void {

    if (action === 'deleteOk') {
      const notification: Notification = new Notification();
      notification.message = `Record with id ${id} deleted`;
      notification.action = action;
      notification.status = true;
      this.notifications.push(notification);
    }

    if (action === 'deleteError') {
      const notification: Notification = new Notification();
      notification.message = `Record with id ${id} do not delete`;
      notification.action = action;
      notification.status = false;
      this.notifications.push(notification);
    }

    if (action === 'createOk') {
      const notification: Notification = new Notification();
      notification.message = 'Record added';
      notification.action = action;
      notification.status = true;
      this.notifications.push(notification);
    }

    if (action === 'createError') {
      const notification: Notification = new Notification();
      notification.message = 'Error while adding record';
      notification.action = action;
      notification.status = false;
      this.notifications.push(notification);
    }

    if (action === 'getOk') {
      const notification: Notification = new Notification();
      notification.message = 'Success while getting data';
      notification.action = action;
      notification.status = true;
      this.notifications.push(notification);
    }

    if (action === 'getError') {
      const notification: Notification = new Notification();
      notification.message = 'Error while getting records';
      notification.action = action;
      notification.status = false;
      this.notifications.push(notification);
    }

    if (action === 'modifyOk') {
      const notification: Notification = new Notification();
      notification.message = `Record with id ${id} modified`;
      notification.action = action;
      notification.status = true;
      this.notifications.push(notification);
    }

    if (action === 'modifyError') {
      const notification: Notification = new Notification();
      notification.message = `Record with id ${id} do not modify`;
      notification.action = action;
      notification.status = false;
      this.notifications.push(notification);
    }

    if (action === 'regOk') {
      const notification: Notification = new Notification();
      notification.message = 'Success registration';
      notification.action = action;
      notification.status = true;
      this.notifications.push(notification);
    }

    if (action === 'regError') {
      const notification: Notification = new Notification();
      notification.message = 'Error registration';
      notification.action = action;
      notification.status = false;
      this.notifications.push(notification);
    }


    if (action === 'loginOk') {
      const notification: Notification = new Notification();
      notification.message = 'Success authentication';
      notification.action = action;
      notification.status = true;
      this.notifications.push(notification);
    }

    if (action === 'loginError') {
      const notification: Notification = new Notification();
      notification.message = 'Error authentication';
      notification.action = action;
      notification.status = false;
      this.notifications.push(notification);
    }

    if (action === 'dataError') {
      const notification: Notification = new Notification();
      notification.message = 'required fields are not filled';
      notification.action = action;
      notification.status = false;
      this.notifications.push(notification);
    }

    if (action === 'equalsPassword') {
      const notification: Notification = new Notification();
      notification.message = 'passwords are not equal';
      notification.action = action;
      notification.status = false;
      this.notifications.push(notification);
    }

    if (action === 'keyError') {
      const notification: Notification = new Notification();
      notification.message = 'error key';
      notification.action = action;
      notification.status = false;
      this.notifications.push(notification);
    }

  }

  public remove(action: string): void {
    let index: number = this.notifications.findIndex(x => x.action == action );
    if (index !== -1) {
      this.notifications.splice(index, 1);
    }
  }

}