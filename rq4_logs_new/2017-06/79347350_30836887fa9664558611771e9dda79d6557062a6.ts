import { Injectable } from '@angular/core';
import 'rxjs/add/operator/map';

import { ToastController, AlertController} from 'ionic-angular';

import { LoggerService } from "../log/logger.service";

@Injectable()
export class MessageService {

  /**
   * @constructor
   * @param toastCtrl ToastController Service provided by Ionic-Angular
   * @param alertCtrl AlertController Service provided by Ionic-Angular
   * @param logger Logger Service
   */
  constructor(private toastCtrl: ToastController,
              private alertCtrl: AlertController,
              private logger: LoggerService){ }

 /**
   * @description Function to show Ionic Toast
   * @param {string} message message to be displayed
   */
  displayToast(message, duration, position) {
    let toast = this.toastCtrl.create({
    message: message,
    duration: duration,
    position: position
  });

  toast.present();
  }
 
}