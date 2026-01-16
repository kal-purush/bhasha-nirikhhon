import { Component, ViewChild } from '@angular/core';
import { Platform,Nav } from 'ionic-angular';
import { StatusBar } from '@ionic-native/status-bar';
import { SplashScreen } from '@ionic-native/splash-screen';
import {FlightSearch} from '../providers/flight-search';
import {HomePage} from '../pages/home/home';
import {AboutPage} from '../pages/about/about';


import { TabsPage } from '../pages/tabs/tabs';


@Component({
  templateUrl: 'app.html',
  providers:[FlightSearch]

})
export class MyApp {
  rootPage:any = TabsPage;

  sortVal:any;


  @ViewChild(Nav) nav:Nav;

  constructor(platform: Platform, statusBar: StatusBar, splashScreen: SplashScreen) {
    platform.ready().then(() => {

      statusBar.styleDefault();
      splashScreen.hide();
    });
    this.sortVal = 'price';
  }
  goToHome(Page){
    this.nav.setRoot(HomePage);
  }
  goToAbout(Page){
    this.nav.setRoot(AboutPage);


  }

  loadSortVal(){
    localStorage.setItem('sortVal',this.sortVal);
    

  }



}