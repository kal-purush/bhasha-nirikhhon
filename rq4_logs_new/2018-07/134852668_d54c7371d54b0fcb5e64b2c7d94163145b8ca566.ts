import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';
import { HomePage } from "../home/home";
import { RedditPage } from '../reddit/reddit';

/**
 * Generated class for the StatsPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-stats',
  templateUrl: 'stats.html',
})
export class StatsPage {

  private homePage = HomePage;
  private redditPage = RedditPage;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
  }

  /**
   * 
   * @param page Open a page
   */
  public openPage(page) {
    this.navCtrl.setRoot(page);
  }

  /**
   * 
   * @param event 
   */
  public swipe(event) {
    // Left
    if(event.direction === 2) {
      this.navCtrl.setRoot(this.homePage);
    }
  }

}