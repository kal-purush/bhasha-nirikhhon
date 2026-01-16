import {Component, ViewChild} from '@angular/core';
import {Platform, NavController} from 'ionic-angular';
import {StatusBar} from '@ionic-native/status-bar';
import {SplashScreen} from '@ionic-native/splash-screen';

import {HomePage} from '../pages/home/home';
import {TabsPage} from "../pages/tabs/tabs";
import {AuthenticationService} from "../service/authentication";

@Component({
  templateUrl: 'app.html'
})
export class MyApp {
  rootPage: any = HomePage;

  @ViewChild('nav') nav: NavController;

  constructor(platform: Platform,
              statusBar: StatusBar,
              splashScreen: SplashScreen,
              private authService: AuthenticationService) {

    // go to tabs page if the user is authenticated
    let authUser = this.authService.getCurrentUser();
    if (authUser) {
      this.rootPage = TabsPage;
    }

    platform.ready().then(() => {

      /*
      Network.onDisconnect().subscribe(() => {
        let alert = this.alertCtrl.create({
          title: "Internet Connection",
          subTitle:"Please Check Your Network connection",
          buttons: [{
            text: 'Ok'
          }]
        });
        alert.present();

      });

      Network.onConnect().subscribe(()=> {
        //console.log('you are online');
      });
      */

      // Okay, so the platform is ready and our plugins are available.
      // Here you can do any higher level native things you might need.
      statusBar.styleDefault();
      splashScreen.hide();
    });
  }
}