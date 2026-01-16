import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';
import {EthAccount} from "../../interfaces/ethAccount";

@IonicPage()
@Component({
  selector: 'page-token-overview',
  templateUrl: 'token-overview.html',
})
export class TokenOverviewPage {

  private ethAccount:EthAccount;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
  }

  ionViewDidLoad() {
    this.ethAccount = this.navParams.get('ethAccount');
  }



}