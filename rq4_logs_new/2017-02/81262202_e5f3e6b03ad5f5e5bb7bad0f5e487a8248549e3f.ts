import { Component, Input } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

import { Post } from '../../../models/post';

@Component({
  selector: 'landing-posts-list',
  templateUrl: 'landing-posts-list.html'
})
export class LandingPostsListPage {
  @Input() posts: Post[];

  constructor(public navCtrl: NavController, public navParams: NavParams) {
    //this.posts.sort().reverse();
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad LandingPostsList');
  }

}