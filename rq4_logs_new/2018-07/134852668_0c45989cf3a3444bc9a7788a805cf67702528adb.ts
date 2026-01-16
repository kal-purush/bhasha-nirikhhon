import {Component} from '@angular/core';
import {IonicPage, NavController, NavParams} from 'ionic-angular';
import {RedditApiService} from "../../providers/reddit/reddit-api-service";

/**
 * Generated class for the RedditPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-reddit',
  templateUrl: 'reddit.html',
})
export class RedditPage {
  loadCompleted: boolean = false;
  public redditG: Array<any>;
  titleOfSearch;

  constructor(public navCtrl: NavController, public navParams: NavParams, private redditApi: RedditApiService) {
    this.titleOfSearch = 'spacex/new';
    this.load(this.titleOfSearch);
  }

  load(url?) {
    this.redditApi.fetch(url).subscribe((data) => {
      this.redditG = data;
      this.loadCompleted = true;
      console.log(data)
    })
  }

  loadMore(infiniteScroll) {
    let lastPost = this.redditG[this.redditG.length - 1];
    if (!lastPost) {
      infiniteScroll.complete()
    } else {
      this.redditApi.fetchNext(lastPost.name, this.titleOfSearch).subscribe((posts) => {
        this.redditG = this.redditG.concat(posts);
        infiniteScroll.complete();
      })
    }
  }


  ionViewDidLoad() {
    console.log('ionViewDidLoad RedditPage');
  }

}