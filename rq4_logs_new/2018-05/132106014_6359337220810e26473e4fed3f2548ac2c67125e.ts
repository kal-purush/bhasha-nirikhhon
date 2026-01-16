import { RootModule } from './client/root.module';
import { Root } from './client/root.component';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { BrowserModule } from '@angular/platform-browser';
import { IdlePreload, IdlePreloadModule } from '@angularclass/idle-preload';
import { CookieModule } from 'ngx-cookie';


// import * as LRU from 'modern-lru';

export function getLRU(lru?: any) {
  // use LRU for node
  // return lru || new LRU(10);
  return lru || new Map();
}
export function getRequest() {
  // the request object only lives on the server
  return { cookie: document.cookie };
}
export function getResponse() {
  // the response object is sent as the index.html and lives on the server
  return {};
}



@NgModule({
  bootstrap: [ Root ],
  imports: [
    BrowserModule.withServerTransition({ 
      appId: 'sample-app'
    }),
    FormsModule,
    RouterModule,
    CookieModule.forRoot(),
    IdlePreloadModule.forRoot(),
  ],
  providers: [

    { provide: 'req', useFactory: getRequest },
    { provide: 'res', useFactory: getResponse },

    { provide: 'LRU', useFactory: getLRU, deps: [] }
  ]
})
export class MainModule {

}