import { NgModule }       from '@angular/core';
import { BrowserModule }  from '@angular/platform-browser';
import { FormsModule }    from '@angular/forms';
import { HttpModule }     from '@angular/http';


import { AppComponent }   from './app.component';
import { routing }        from './app.routing';
import { DashboardComponent }   from './dashboard.component';
import {HeroesModule} from "./heroes/heroes.module";
import {CrisisCenterModule} from "./crisis-center/crisis-center.module";

@NgModule({
    imports: [
        BrowserModule,
        FormsModule,
        routing,
        HttpModule,
        HeroesModule,
        CrisisCenterModule
    ],
    declarations: [
        AppComponent,
        DashboardComponent
    ],
    providers: [
    ],
    bootstrap: [ AppComponent ]
})
export class AppModule {
}