import { Component , NgZone } from '@angular/core';
import { Platform } from 'ionic-angular';
import { StatusBar, Splashscreen } from 'ionic-native';

import { TabsPage } from '../pages/tabs/tabs';
import { InfoModalPage } from '../pages/info-modal/info-modal' ;
// this one for the auth components 
import { AuthData } from '../providers/auth-data';
//this one is for showing loding ... message 
import { LoadingController } from 'ionic-angular';
//import { firebaseConfig } from './firebase.config';
import firebase from 'firebase';

import { LoginPage } from '../pages/login/login';
import { SignupPage } from '../pages/signup/signup';
import { HomePage } from '../pages/home/home';
import { ResetPasswordPage } from '../pages/reset-password/reset-password';
import { FriendsPage } from '../pages/friends/friends';
import { CommentsPage } from '../pages/comments/comments';
import { FindFriendsPage } from '../pages/fiend-friends/fiend-friends';
//import { FindFriendsPage } from '../fiend-friends/fiend-friends';
import { AboutPage } from '../pages/about/about';
 



// @Component({
//   templateUrl: 'app.html'
// })
// export class MyApp {
//  // by defult it will be the log in page unless he is logged in 
//   rootPage: any = LoginPage;
//   loader: any;
// // we will inject the component
//   constructor(public auth: Auth,   public loadingCtrl: LoadingController )  {
//     this.presentLoading();
//     // we will cheak if the user is auth or not 
//       this.auth.login().then((isLogedin) => {
//       // if it auth render the tabs page 
//         if(isLogedin){
//           this.rootPage = TabsPage;
//         }else {
//           //if not let him sign in first 
//           this.rootPage = LoginPage;
//         }

//         this.loader.dismiss();
//      });
//   }

//     presentLoading() {
//     this.loader = this.loadingCtrl.create({
//       content: "Please wait...",
//     });
//     this.loader.present();
//   }




// }

@Component({
  templateUrl: 'app.html'
})
export class MyApp {
  rootPage :any;
  zone: NgZone;

  constructor(platform: Platform) { 
    firebase.initializeApp({
    apiKey: "AIzaSyAseZYIB_FzpV-GuKD3TaL2Ykca9BM8ME8",
    authDomain: "post-map-f2fbc.firebaseapp.com",
    databaseURL: "https://post-map-f2fbc.firebaseio.com",
    storageBucket: "post-map-f2fbc.appspot.com",
    messagingSenderId: "791954812338"
});
      this.zone = new NgZone({});
      const unsubscribe = firebase.auth().onAuthStateChanged((user) => {
      this.zone.run( () => {
    if (!user) {
      this.rootPage = LoginPage;
      unsubscribe();
    } else { 
      this.rootPage = TabsPage ; 
      unsubscribe();
    }
  });     
});
    platform.ready().then(() => {
      // Okay, so the platform is ready and our plugins are available.
      // Here you can do any higher level native things you might need.
      StatusBar.styleDefault();
      Splashscreen.hide();
    });

    
  }   
}