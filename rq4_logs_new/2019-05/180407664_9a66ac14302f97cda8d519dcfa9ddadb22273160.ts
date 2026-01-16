import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';
import { PollHistoryPage } from '../poll-history/poll-history';
import { PollBuilderServiceProvider } from '../../providers/poll-builder-service/poll-builder-service';

/**
 * Generated class for the PollBuilder4Page page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-poll-builder4',
  templateUrl: 'poll-builder4.html',
})
export class PollBuilder4Page {

  constructor(public navCtrl: NavController, public navParams: NavParams, public BuilderService: PollBuilderServiceProvider) {
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad PollBuilder4Page');
  }

  publishPoll() {
    this.navCtrl.push(PollHistoryPage);
  }

}