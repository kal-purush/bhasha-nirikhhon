import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule }   from '@angular/forms';
import { HttpModule }    from '@angular/http';

import { AppRoutingModule } from './app-routing.module';

import { AppComponent }          from './app.component';
import { DashboardComponent }    from './dashboard/dashboard.component';
import { HeroesComponent }       from './heroes/heroes.component';
import { HeroDetailComponent }   from './hero-detail/hero-detail.component';
import { HeroService }           from './service/hero.service';
import { HeroSearchComponent }   from './hero-search/hero-search.component';
import { HomeComponent }         from './home/home.component';
import { AuthenticationService } from './service/authentication.service';
import { LoginComponent }        from './login/login.component';

@NgModule({
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule,
    AppRoutingModule
  ],
  declarations: [
    AppComponent,
    DashboardComponent,
    HeroDetailComponent,
    HeroesComponent,
    HeroSearchComponent,
    HomeComponent,
    LoginComponent
  ],
  providers: [ HeroService, AuthenticationService ],
  bootstrap: [ AppComponent ]
})
export class AppModule { }