import {BrowserModule} from '@angular/platform-browser'
import {NgModule} from '@angular/core'
import {FormsModule} from '@angular/forms'
import {HttpModule} from '@angular/http'

import {AppRoutingModule} from './AppRoutingModule'
import {AppComponent} from './AppComponent'
import {HeroesComponent} from './HeroesComponent'
import {HeroDetailComponent} from './HeroDetailComponent'
import {HeroService} from './HeroService'
import {DashboardComponent} from './DashboardComponent'

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
    HeroesComponent,
    HeroDetailComponent
  ],
  providers: [HeroService],
  bootstrap: [AppComponent]
})
export class AppModule {

}