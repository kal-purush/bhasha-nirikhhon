/**
 * @module Ng2Example2
 * @preferred
 */ /** */

// lib imports
import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
// external imports
import {LazyLoadComponent} from 'helpers/lazy-load-component';
// internal imports
import {Ng2Example2} from './component';
// exports
export * from './globals';

@NgModule({
    imports: [
        CommonModule
    ],
    providers: [
        {provide: LazyLoadComponent, useValue: Ng2Example2}
    ],
    declarations: [
        Ng2Example2
    ],
    entryComponents: [
        Ng2Example2
    ]
})
export class Ng2Example2Module {}