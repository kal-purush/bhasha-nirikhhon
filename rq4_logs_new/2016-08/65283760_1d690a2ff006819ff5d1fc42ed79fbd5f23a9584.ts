import { NgModule }       from '@angular/core';
import { BrowserModule }  from '@angular/platform-browser';

import {AppComponent}   from './app.component';
import {ReaderComponent} from "./reader.component";
import {routing} from "./app.routes";
import {FavoritesComponent} from "./favorites.component";
import {ProfileComponent} from "./profile.component";

@NgModule({
    imports: [
        BrowserModule,
        routing
    ],
    declarations: [
        AppComponent,
        ReaderComponent,
        FavoritesComponent,
        ProfileComponent
    ],
    bootstrap: [ AppComponent ]
})

export class AppModule {}