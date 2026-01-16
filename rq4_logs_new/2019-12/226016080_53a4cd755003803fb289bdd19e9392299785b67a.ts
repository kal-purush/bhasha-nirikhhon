import { Injectable } from '@angular/core';
import { ToastController, AlertController, LoadingController } from '@ionic/angular';

@Injectable({
  providedIn: 'root'
})
export class AlertService {
  toastOnce: any;
  loadingOnce: any;
  constructor(
    private toastController: ToastController,
    private alertController: AlertController,
    private loadingController: LoadingController
  ) { }
  async presentToast(message: any) {
    const toast = await this.toastController.create({
      message,
      duration: 4000,
      position: 'top',
      animated: true,
      mode: 'ios'
    });
    toast.present();
  }

  async presentToastWithClose(message: any, closeText: any) {
    const toast = await this.toastController.create({
      message,
      position: 'top',
      animated: true,
      mode: 'ios',
      buttons: [
        {
          side: 'end',
          text: closeText,
          handler: () => {
          }
        }
      ]
    });
    toast.present();
  }

  async presentOnceToast(headerContent, messageContent, viewText, closeText, callback) {
    if (this.toastOnce) {
      this.toastOnce.dismiss();
    }

    this.toastOnce = await this.toastController.create({
      header: headerContent,
      message: messageContent,
      position: 'top',
      duration: 4000,
      animated: true,
      mode: 'ios',
      buttons: [
        {
          side: 'end',
          text: viewText,
          handler: () => {
            callback();
          }
        },
        {
          side: 'start',
          text: closeText,
          handler: () => {
          }
        }
      ]
    });
    this.toastOnce.present();
  }

  dismissOnceToast() {
    if (this.toastOnce) {
      this.toastOnce.dismiss();
    }
  }

  async presentInputPrompt(headerText, placeHolderText, OkText, onResult) {
    const alert = await this.alertController.create({
      header: headerText,
      mode: 'ios',
      inputs: [
        {
          name: 'name1',
          type: 'text',
          placeholder: placeHolderText
        }],
      buttons: [
        {
          text: OkText,
          handler: (data) => {
            onResult(data.name1);
          }
        }
      ]
    });

    await alert.present();
  }

  async presentOnceLoading(messageContent) {
    if (this.loadingOnce) {
      this.loadingOnce.dismiss();
    }

    this.loadingOnce = await this.loadingController.create({
      spinner: 'crescent',
      message: messageContent,
      translucent: true
    });
    await this.loadingOnce.present();
  }


  async alertPopup(header, message) {
    const alert = await this.alertController.create({
      header,
      mode: 'ios',
      message,
      buttons: ['OK']
    });
    await alert.present();
  }

  dismissOnceLoading() {
    if (this.loadingOnce) {
      this.loadingOnce.dismiss();
    }
  }

}