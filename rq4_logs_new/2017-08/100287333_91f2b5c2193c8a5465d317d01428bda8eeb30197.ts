import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';

/**
 * Generated class for the GaleriPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-galeri',
  templateUrl: 'galeri.html',
})
export class GaleriPage {

  public src_1: any;
  public src_2: any;
  public src_3: any;
  public src_4: any;
  public src_5: any;
  public src_6: any;
  public src_7: any;
  public src_8: any;
  public src_9: any;

  constructor(
    public navCtrl: NavController, 
    public navParams: NavParams,
  ) {
    this.src_1 = 'assets/image/1.jpg';
    this.src_2 = 'assets/image/2.jpg';
    this.src_3 = 'assets/image/3.jpg';
    this.src_4 = 'assets/image/4.jpg';
    this.src_5 = 'assets/image/5.jpg';
    this.src_6 = 'assets/image/6.jpg';
    this.src_7 = 'assets/image/7.jpg';
    this.src_8 = 'assets/image/8.jpg';
    this.src_9 = 'assets/image/9.jpg';
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad GaleriPage');
  }

}