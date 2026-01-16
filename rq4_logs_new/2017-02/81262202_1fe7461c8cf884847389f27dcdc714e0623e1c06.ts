import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

import { Post } from '../../../models/post';

@Component({
  selector: 'page-post-details',
  templateUrl: 'post-details.html'
})
export class PostDetailsPage {
  post: Post[];

  constructor(public navCtrl: NavController, public navParams: NavParams) {
      this.post = this.navParams.get('post');
      console.log(this.post);
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad PostDetailsPage');
  }

}