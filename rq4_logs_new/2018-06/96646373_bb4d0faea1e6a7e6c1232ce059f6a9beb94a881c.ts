import { Component } from '@angular/core';
import { NavParams, ViewController } from "ionic-angular";


/**
 * Generated class for the EditRadiusPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */
@Component({
  selector: 'page-edit-radius',
  templateUrl: 'edit-radius.html',
})
export class EditRadiusPage {
  private radius: number;

  constructor(public viewCtrl: ViewController, public navParams: NavParams) {
    this.radius = navParams.get("radius");
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad EditRadiusPage');
  }

  dismissDontSave() {
    this.viewCtrl.dismiss({ "radius": this.navParams.get("radius") });
  }

  dismissSave() {
    this.viewCtrl.dismiss({ "radius": this.radius });
  }


}