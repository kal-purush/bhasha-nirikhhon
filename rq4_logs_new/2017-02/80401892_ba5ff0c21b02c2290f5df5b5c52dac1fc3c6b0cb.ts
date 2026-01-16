import { Component } from '@angular/core';
import { AllRequestPage } from '../all-request/request';
import { NavController } from 'ionic-angular';

@Component({
  selector: 'dashboard',
  templateUrl: 'dashboard.html'
})
export class Dashboard {

  constructor(public navCtrl: NavController) {

  }

  goToAllRequest() {
    this.navCtrl.setRoot(AllRequestPage);
  }

}