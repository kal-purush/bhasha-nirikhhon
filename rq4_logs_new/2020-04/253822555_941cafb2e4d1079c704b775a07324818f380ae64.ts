import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';

import {IonicModule} from '@ionic/angular';

import {SignupPageRoutingModule} from './signup-routing.module';

import {SignupPage} from './signup.page';
import {IntroductionPageModule} from '../introduction/introduction.module';
import {SearchCityPageModule} from '../search-city/search-city.module';
import {MaterialModule} from '../../material.module';
import {MAT_DATE_LOCALE} from '@angular/material/core';

@NgModule({
    imports: [
        CommonModule,
        FormsModule,
        IonicModule,
        SignupPageRoutingModule,
        IntroductionPageModule,
        SearchCityPageModule,
        MaterialModule,
        ReactiveFormsModule
    ],
    providers: [
        {provide: MAT_DATE_LOCALE, useValue: 'fr-FR'},
    ],
    declarations: [SignupPage]
})
export class SignupPageModule {}