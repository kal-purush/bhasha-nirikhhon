import { NgModule, CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { provideAuth } from "angular2-jwt"
import { HttpModule } from "@angular/http"
import { NgSemanticModule } from "ng-semantic"

import { AppComponent }  from './app.component'
import { HomeMainPage } from './components/home_main_page.component'
import { HomeOrgPage } from './components/org/home_org_page.component'

//Composants page organisme
import { DataItemsNavbar } from './components/org/data_items/navbar_data_items.component'
import { AttrsDataChoicePage } from './components/org/data_items/attrs_data_choice_page.component'
import { LocalisationChoicePage } from './components/org/data_items/localisation_choice_page.component'
import { SpeciesChoicePage } from './components/org/data_items/species_choice_page.component'

import { routing } from "./routes";

import { ServiceCaterogiesModules } from "./services/modules/categories.service"

@NgModule({
    imports: [
        BrowserModule,
        HttpModule,
        NgSemanticModule,
        routing
    ],
    providers: [
        provideAuth({
            globalHeaders: [{"Content-type": "application/json"}],
            newJwtError: true,
            noTokenScheme: true
        }),
        ServiceCaterogiesModules
    ],
    declarations: [ 
        AppComponent,
        HomeMainPage,
        HomeOrgPage,
        DataItemsNavbar,
        AttrsDataChoicePage,
        LocalisationChoicePage,
        SpeciesChoicePage
    ],
    bootstrap:    [ AppComponent ],
    schemas: [
        CUSTOM_ELEMENTS_SCHEMA
    ]
})
export class AppModule {}