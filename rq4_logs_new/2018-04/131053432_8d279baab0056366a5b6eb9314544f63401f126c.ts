import {BrowserModule} from '@angular/platform-browser';
import {NgModule} from '@angular/core';
import {HttpClientModule} from '@angular/common/http';
import {AppRoutes} from './app_routes';
import {NetrunnerModule} from './../netrunner/netrunner_module';


import { AppComponent } from './app.component';
@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule, AppRoutes, HttpClientModule, NetrunnerModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }