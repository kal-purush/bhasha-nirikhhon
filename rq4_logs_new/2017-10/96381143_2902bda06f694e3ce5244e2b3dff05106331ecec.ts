import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Http } from '@angular/http';

import { OpenfactSyncModule } from 'ngo-openfact-sync';

import { EditRoutingModule } from './edit-routing.module';
import { EditComponent } from './edit.component';

import { DocumentSearchToolbarModule } from './../search-toolbar/document-search-toolbar.module';

@NgModule({
  imports: [
    CommonModule,
    EditRoutingModule,
    OpenfactSyncModule,
    DocumentSearchToolbarModule
  ],
  declarations: [EditComponent],
})
export class EditModule {
  constructor(http: Http) {
  }
}