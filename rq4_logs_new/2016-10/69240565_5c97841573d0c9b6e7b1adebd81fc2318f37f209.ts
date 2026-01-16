import { NgModule } from '@angular/core';
import { BrowserModule }  from '@angular/platform-browser';

import { HttpModule } from '@angular/http';

import { AppComponent } from './app.component';
import { LoginModule } from './login/login.module';
import { HeroModule } from './hero/hero.module';
import { CoreModule } from './core/core.module';
import { routing } from './app.routing';

import '../public/styles/common.scss';

@NgModule({
  imports: [
    BrowserModule,
    HttpModule,
    LoginModule,
    HeroModule,
    CoreModule.forRoot({username: ''}),
    routing
  ],
  declarations: [ AppComponent ],
  bootstrap: [ AppComponent ]
})
export class AppModule { }