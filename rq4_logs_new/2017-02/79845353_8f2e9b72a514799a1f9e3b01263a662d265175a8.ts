import {Injectable, Input} from '@angular/core';
import 'rxjs/add/operator/map';
import {AlertController, ToastController} from "ionic-angular";

@Injectable()
export class MessageService {

  constructor(public alertController: AlertController, public toastController: ToastController) {
  }

  public showToast(message: string): void {
    let toast = this.toastController.create({
      // message: this.translate.instant(message),
      message: message,
      duration: 3000,
      position: 'top'
    });

    toast.present();
  }
}