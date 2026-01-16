import { Component, ViewChild } from '@angular/core';
import { Nav, Platform , MenuController } from 'ionic-angular';
import { StatusBar, Splashscreen } from 'ionic-native';

import { MyProfilePage } from '../pages/my-profile/my-profile';
import { CheckInStoryPage } from '../pages/check-in-story/check-in-story';
import { BookStoryPage } from '../pages/book-story/book-story';
import { MyOptionsPage } from '../pages/my-options/my-options';
import { MyFavouritePage } from '../pages/my-favourite/my-favourite';
import { OrderStoryPage } from '../pages/order-story/order-story';
import { OrderPlaceStoryPage } from '../pages/order-place-story/order-place-story';
import { HomePage } from '../pages/home/home';
import { CheckinPage } from '../pages/checkin/checkin';
import { DeliveryPage } from '../pages/delivery/delivery';
import { LoginPage } from '../pages/login/login';


@Component({
  templateUrl: 'app.html'
})
export class MyApp {
  @ViewChild(Nav) nav: Nav;

  rootPage: any = LoginPage;

  sections: Array<{title: string, component: any, icon: string}>;
  pages: Array<{title: string, component: any, icon: string}>;

  constructor(public platform: Platform,public menuCtrl: MenuController) {
    this.initializeApp();
    this.sections = [      
      { title: 'Заведения', component: HomePage, icon: "checkin"},
      { title: 'Check In', component: CheckinPage, icon: "checkin"},
      { title: 'Доставка', component: DeliveryPage, icon: "checkin"},
      { title: 'Избранное', component: MyFavouritePage, icon: "checkin"},
      { title: 'История', component: CheckInStoryPage, icon: "checkin"}
    ];
    this.pages = [
      { title: 'Мой профиль', component: MyProfilePage, icon: "profile" },
      { title: 'Настройки', component: MyOptionsPage, icon: "wrench" }
    ]

  }

  initializeApp() {
    this.platform.ready().then(() => {
      StatusBar.styleDefault();
      Splashscreen.hide();
    });
  }

  openPage(page) {
    this.nav.setRoot(page.component);
  }
  logoutApp(){
    this.menuCtrl.swipeEnable(false);
    this.menuCtrl.enable(false, 'menu');
    this.nav.setRoot(LoginPage);    
  }
}