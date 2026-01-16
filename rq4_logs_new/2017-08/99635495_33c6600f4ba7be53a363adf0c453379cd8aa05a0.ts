import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import { Produce } from '../produce-list/produce';
import { AngularFireDatabase, FirebaseListObservable } from 'angularfire2/database';
import * as humanize from 'humanize';
import { AlertController } from 'ionic-angular';
import {ToastController} from 'ionic-angular';

/**
 * Generated class for the DeliveryDateAddressPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */

@Component({
  selector: 'page-delivery-date-address',
  templateUrl: 'delivery-date-address.html',
})
export class DeliveryDateAddressPage {
  item = {} as Produce;
  private orders: FirebaseListObservable<any[]>;

  constructor(public navCtrl: NavController, public navParams: NavParams, public af_db: AngularFireDatabase,public alertCtrl: AlertController,public toastCtrl: ToastController  ) {
    this.item = navParams.data;
    this.item.deliveryDate = new Date().toISOString();
    this.orders = af_db.list('/orders');
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad DeliveryDateAddressPage');
  }

  save() {
    let self = this;
    this.item.date = humanize.time();
    this.item.deliveryDate = new Date(this.item.deliveryDate).toDateString();


    this.orders.push(this.item).then(
      function (resolve) {
        let toast = self.toastCtrl.create(

          {
            message: "Order Placed!",
            duration: 1500,
            position: 'top'
          }
        );

        toast.present(toast);
        self.navCtrl.popToRoot();
        //self.navCtrl.push(DeliveryDateAddressPage, self.item);
      }

      ,
      function (error) {
        let alert = self.alertCtrl.create({
          title: 'Error',
          subTitle: error.message,
          buttons: ['OK']
        });


        alert.present();
        

      }
    );


  }


}