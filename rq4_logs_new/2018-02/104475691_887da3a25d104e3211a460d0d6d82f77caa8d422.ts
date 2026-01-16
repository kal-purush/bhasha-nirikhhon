import { Component } from '@angular/core';
import {IonicPage, LoadingController, NavController, NavParams, ToastController, ViewController} from 'ionic-angular';
import {Place} from "../../app/model/place.model";
import {PlaceService} from "../../app/providers/place.service";

/**
 * Generated class for the AddPlacePage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-add-place',
  templateUrl: 'add-place.html',
})
export class AddPlacePage {
  private _place: Place;

  constructor(public navCtrl: NavController,
              public viewCtrl: ViewController,
              public navParams: NavParams,
              public toastCtrl: ToastController,
              private _placeService: PlaceService,
              private loadingCtrl: LoadingController) {
    this._place = new Place();
  }

  ionViewDidLoad() {
    if(this.navParams.get('place')) {
      this._place = this.navParams.get('place');
    }
  }

  public _showToastWithCloseButton(message: string): void {
    const toast = this.toastCtrl.create({
      message: message,
      showCloseButton: true,
      closeButtonText: 'Ok'
    });
    toast.present();
  }

  public _addPlace(place: Place): void {
    let loading = this.loadingCtrl.create({
      spinner: 'bubbles',
      content: 'Ajout en cours ...'
    });

    loading.present();

    this._placeService.createPlace(place)
      .subscribe(res => {
        loading.dismiss();
        this.navCtrl.pop();
      });
  }
}