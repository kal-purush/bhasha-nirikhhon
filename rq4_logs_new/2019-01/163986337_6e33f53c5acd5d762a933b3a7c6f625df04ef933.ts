import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { HttpClientModule } from '@angular/common/http';
import { RouterModule } from '@angular/router';
import { StoreRouterConnectingModule } from '@ngrx/router-store';
import { StoreModule } from '@ngrx/store';

import { routes } from './app.routing';
import { syncReducers } from './app.reducers';
import { AppComponent } from './app.component';
import { NotFoundComponent } from '../../shared/components/not-found/not-found.component';

import { HeaderModule } from '../header/header.module';
import { ContentModule } from '../content/content.module';

import { TransferHttpService } from '../../shared/services/transfer-http.service';

export const MODULE_DECLARATIONS = [
  AppComponent,
  NotFoundComponent
];

export const MODULE_IMPORTS = [
  BrowserModule,
  HttpClientModule,
  RouterModule.forRoot(routes, {useHash: false}),
  StoreModule.forRoot(syncReducers),
  StoreRouterConnectingModule.forRoot({stateKey: 'router'}),
  HeaderModule,
  ContentModule
];

export const MODULE_PROVIDERS = [
  TransferHttpService
];

@NgModule({
  declarations: MODULE_DECLARATIONS,
  imports: MODULE_IMPORTS,
  providers: MODULE_PROVIDERS,
  bootstrap: [AppComponent],
  exports: [AppComponent]
})
export class AppModule {
  constructor() { }
}