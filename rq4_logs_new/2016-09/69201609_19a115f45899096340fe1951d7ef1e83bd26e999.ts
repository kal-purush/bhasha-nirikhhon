import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { AppComponent } from './app.component';
import { AngularFireModule, AuthProviders, AuthMethods } from 'angularfire2';

// Must export the config
export const firebaseConfig = {
  apiKey: "AIzaSyA7rfs9Heh5ohODV_FmHq4jqhDCVnSmZKU",
  authDomain: "i-am-ready-60c46.firebaseapp.com",
  databaseURL: "https://i-am-ready-60c46.firebaseio.com/",
  storageBucket: "gs://i-am-ready-60c46.appspot.com"
};

const myFirebaseAuthConfig = {
   provider: AuthProviders.Anonymous,
  method: AuthMethods.Anonymous,
}


@NgModule({
  imports: [
    BrowserModule,
    AngularFireModule.initializeApp(firebaseConfig, myFirebaseAuthConfig)
  
  ],
  declarations: [ AppComponent ],
  bootstrap: [ AppComponent ]
})
export class AppModule {}