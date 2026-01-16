import { Component, ViewChild } from '@angular/core';
import { Platform, Nav } from 'ionic-angular';
import { StatusBar } from '@ionic-native/status-bar';
import { SplashScreen } from '@ionic-native/splash-screen';


import { UserPage } from '../pages/user/user';
import { LoginPage } from '../pages/login/login';

@Component({
  // O selector encapsula as alterações do scss
  selector:'myApp',
  templateUrl: 'app.html'
})
export class MyApp {
  // Recuperar o elemento de navegação do template
  @ViewChild(Nav) public nav:Nav;
  rootPage:any = LoginPage;

  // Define as paginas que podem ser acessadas do menu
  public paginas = [
    {titulo : "Meu Perfil", componente: UserPage, icone:'person',}
  ];

  constructor(platform: Platform, statusBar: StatusBar, splashScreen: SplashScreen) {
    platform.ready().then(() => {
      // Okay, so the platform is ready and our plugins are available.
      // Here you can do any higher level native things you might need.
      statusBar.styleDefault();
      splashScreen.hide();
    });
  }

  irParaPagina(componente){
    this.nav.push(componente);
  }
}
