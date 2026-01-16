import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { IonicModule } from '@ionic/angular';

import {PreloadAllModules, RouteReuseStrategy, RouterModule} from '@angular/router';

import {IonicRouteStrategy} from "@ionic/angular/standalone";
import {routes} from "./app.routes";
import {AppComponent} from "./app.component";
import {MenuPrincipalComponent} from "./components/menu-principal/menu-principal.component";

@NgModule({
  declarations: [
    AppComponent,

  ],
  imports: [
    BrowserModule,
    // IonicModule.forRoot(),
    RouterModule.forRoot(routes, {preloadingStrategy: PreloadAllModules}),
    MenuPrincipalComponent,
    IonicModule,
    // IonicModule
  ],
  bootstrap: [AppComponent],
  providers: [
    { provide: RouteReuseStrategy, useClass: IonicRouteStrategy }
  ],
})
export class AppModule {}