import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

/**
 * Generated class for the RoiCalcPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */

@Component({
  selector: 'page-roi-calc',
  templateUrl: 'roi-calc.html',
})
export class RoiCalcPage {

	startDate:any;
	endDate:any;
	inp_inv:number;
	inp_return:number;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad RoiCalcPage');
  }

}