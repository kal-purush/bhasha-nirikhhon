import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { RouterModule } from '@angular/router';
import { HttpClientModule, provideHttpClient, withFetch } from '@angular/common/http';

import { AppComponent } from './app.component';
import { HomeComponent } from './home/home.component';
import { SurveyComponent } from './survey/survey.component';
import { ListAllComponent } from './list-all/list-all.component';
import { routes } from './app.routes'; // Import routes

@NgModule({
  declarations: [
    AppComponent,
    HomeComponent,
    SurveyComponent,
    ListAllComponent
  ],
  imports: [
    BrowserModule,
    RouterModule.forRoot(routes), // Use imported routes here
    HttpClientModule
  ],
  providers: [
    provideHttpClient(withFetch()) // Add fetch API support here
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }