import {BrowserModule} from "@angular/platform-browser";
import {FormsModule}   from '@angular/forms';
import {NgModule} from "@angular/core";
import {HttpModule} from '@angular/http';

import {routing} from "./app.routes"
import {Calendar} from "./calendar/calendar.component";
import {EntryForm} from "./entryForm/entry-form.component";
import {Login} from "./login/login.component";
import {MonthlyViewComponent} from "./monthly/monthly.component";
import {SignUp} from "./signup/signup.component";
import {YearlyViewComponent} from "./yearly/yearly.component";

import {AppComponent} from "./app.component";

@NgModule({
    imports: [
        BrowserModule,
        FormsModule,
        routing,
        HttpModule,
    ],
    declarations: [
        AppComponent,
        Calendar,
        EntryForm,
        Login,
        MonthlyViewComponent,
        SignUp,
        YearlyViewComponent
    ],
    bootstrap: [AppComponent]
})
export class AppModule {
}