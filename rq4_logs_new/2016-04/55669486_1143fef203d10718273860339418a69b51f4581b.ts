import {Page, NavController} from 'ionic-angular';
import {GettingStartedPage} from '../getting-started/getting-started';

/*
  Generated class for the IntroPage page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Page({
  templateUrl: 'build/pages/intro/intro.html',
})
export class IntroPage {
  nav: NavController;

  static get parameters() {
    return [[NavController]];
  }

  constructor(nav: NavController){
      this.nav = nav;
  }

  goToHome(){
      this.nav.setRoot(GettingStartedPage);
  }
}