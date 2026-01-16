import { Component, ViewChild } from '@angular/core';
import { Nav, Platform } from 'ionic-angular';
import { StatusBar } from '@ionic-native/status-bar';
import { SplashScreen } from '@ionic-native/splash-screen';
import { AngularFireAuth } from 'angularfire2/auth';
import { LoginPage } from '../pages/login/login';



@Component({
  templateUrl: 'app.html'
})
export class MyApp {
  uid: string;
  nombre: String;
  @ViewChild(Nav) nav: Nav;

  rootPage  = LoginPage;

  pages: Array<{title: string, component: any}>;
  pagesAdmin: Array<{title: string, component: any, icono: any}>;

  constructor(public platform: Platform, public statusBar: StatusBar, 
    public splashScreen: SplashScreen,
    private afAuth: AngularFireAuth) {
    this.initializeApp();

    // used for an example of ngFor and navigation
    this.pages = [
    // { title: 'Salir', component: HomePage },
    // { title: 'List', component: ListPage }
    ]

    this.pagesAdmin = [
      { title: 'Crear Estación de Trabajo', component: 'MisestacionesPage', icono: 'checkbox-outline' }
      ];
  }

  initializeApp() {
    this.platform.ready().then(() => {
      // Okay, so the platform is ready and our plugins are available.
      // Here you can do any higher level native things you might need.
      this.statusBar.styleDefault();
      this.splashScreen.hide();
    });
  }

  openPage(page) {
    // Reset the content nav to have just this page
    // we wouldn't want the back button to show in this scenario
    this.nav.setRoot(page.component);
  }

   
salir(){

  this.afAuth.auth.signOut();
  this.platform.exitApp();
  this.nav.setRoot('LoginPage');
}
}