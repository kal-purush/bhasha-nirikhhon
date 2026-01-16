import {
    NgModule
} from '@angular/core'

import {
    HttpModule
} from '@angular/http'

import {
    ROUTER_DIRECTIVES
} from '@angular/router'

import {
    BrowserModule
} from '@angular/platform-browser'

import {
    REACTIVE_FORM_DIRECTIVES,
    FormsModule,
    ReactiveFormsModule
} from '@angular/forms'


import {
    routing
} from './routes'
import {
    DatasetViewComponent
} from './datasetView/datasetView.component'
import {
    ListsComponent
} from './list/list.component'
import {
    DatasetEditComponent
} from './edit/edit.component'

import {
    TestGuard
} from './guard'

import {
    EditDeactivateGuard
} from './guard'

import {
    NewDatasetComponent
} from './new/new.component'

import {
    DatasetService
} from './service/dataset.service'

import {
    SearchPipe
} from './list/pipe/search.pipe'

import {
    DatasetHomeComponent
} from './home.component'


@NgModule({
    imports: [BrowserModule, routing, FormsModule, HttpModule, ReactiveFormsModule], // array of modules needed for this modules
    //components and directives part of this module
    declarations: [DatasetHomeComponent, DatasetViewComponent, ListsComponent, DatasetEditComponent, NewDatasetComponent, SearchPipe],
    //root component of the app
    bootstrap: [DatasetHomeComponent],
    //services used across the mpdule
    providers: [DatasetService]
})

export class DatasetModule {

}