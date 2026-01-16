import {
    NgModule
} from '@angular/core'
import {
    HttpModule
} from '@angular/http'

import {
    RouterModule
} from '@angular/router'

import {
    BrowserModule
} from '@angular/platform-browser'


import {
    HomeComponent
}
from './home.component'

import {
    routing
} from './routes'

import {
    DatasetModule
} from '../dataset/dataset.module'

import {
    AdvertiseComponent
}
from './advertise/advertise.component'


@NgModule({
    imports: [BrowserModule, routing, RouterModule, HttpModule, DatasetModule],
    declarations: [HomeComponent, AdvertiseComponent],
    bootstrap: [HomeComponent]
})

export class HomeModule {

}