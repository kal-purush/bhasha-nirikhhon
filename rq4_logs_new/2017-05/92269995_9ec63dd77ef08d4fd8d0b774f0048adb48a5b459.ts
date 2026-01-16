import { Injectable } from '@angular/core';
import { Loading, LoadingController } from 'ionic-angular';

// Messages used by the LoadingService.
const messages = {
  defaultLoadingMsg: 'Loading...'
};

/**
 * Class Managing the Loading Screen of the application.
 */
@Injectable()
export class LoadingServiceProvider {

  private loading: Loading;

  /**
   * Used to show the loading screen.
   * @param {string} message - Message to be shown while loading. Default is "Loading..."
   */
  public showLoading(message:string=messages.defaultLoadingMsg) {
    this.loading = this.loadingCtrl.create({
      content: message,
      dismissOnPageChange: true
    });
    this.loading.present();
  }


  /**
   * Dismisses the loading screen.
   */
  public dismissLoading() {
    this.loading && this.loading.dismiss();
  }


  /**
   * @constructor
   * @param {LoadingController} loadingCtrl - LoadingController from ionic-angular.
   */
  constructor(private loadingCtrl: LoadingController) {}

}