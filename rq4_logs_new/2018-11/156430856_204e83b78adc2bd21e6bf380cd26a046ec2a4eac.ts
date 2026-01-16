import { Injectable } from '@angular/core';
import { ToastController, LoadingController } from 'ionic-angular';

@Injectable()
export class UtilitiesProvider {

  constructor(private toast: ToastController, private loading: LoadingController) {
    console.log('Hello UtilitiesProvider Provider');
  }

  /**
   * Shows a toast
   * @param {string} message
   * @param {*} duration
   * @memberof UtilitiesProvider
   */
  showToast(message: string, duration) {
    this.toast.create({
      message: message,
      duration: duration      
    }).present();        
  }

  /**
   * Get a loading controller
   * @param {string} message
   * @returns
   * @memberof UtilitiesProvider
   */
  getLoading(message: string) {
      return this.loading.create({
        content: message,
        spinner: 'crescent'
      });
  }    

}