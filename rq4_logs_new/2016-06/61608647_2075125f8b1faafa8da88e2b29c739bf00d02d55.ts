import {Component} from '@angular/core';
import {Platform, ionicBootstrap,  MenuController} from 'ionic-angular';
import {StatusBar} from 'ionic-native';
import {TabsPage} from './pages/tabs/tabs';
import {InMemoryMockDataService} from './mockData/mock_comments';
import {LoginPage} from './pages/login-page/login-page';
import {CHART_DIRECTIVES} from 'ng2-charts/ng2-charts';

import {HTTP_PROVIDERS} from '@angular/http';
import {ROUTER_PROVIDERS} from '@angular/router';
import {OAuthService} from './service/oauthService';

@Component({
  templateUrl: 'build/app.html'
})
export class MyApp {

  private rootPage:any;

  constructor(private platform:Platform, 
              private  menu: MenuController,
              private oauthService: OAuthService) {

    var storage = window.localStorage;
    menu.enable(true);
      // TODO: Verify the token
      // if token not exsist
      // TODO:if expired go refrsh token
    if (storage.getItem("AccessToken") == null) {
      //storage.setItem("AccessToken","test");
      this.rootPage = LoginPage;
    }else {
      this.rootPage = LoginPage;
    }

    platform.ready().then(() => {
      // Okay, so the platform is ready and our plugins are available.
      // Here you can do any higher level native things you might need.
      StatusBar.styleDefault();
    });
  }

  public login(): void {
      this.oauthService.login();
  }

  public logout(): void {
    this.oauthService.logoff();
  }
}

var providers = [  
    HTTP_PROVIDERS,
    ROUTER_PROVIDERS,
    OAuthService
];

ionicBootstrap(MyApp, providers)