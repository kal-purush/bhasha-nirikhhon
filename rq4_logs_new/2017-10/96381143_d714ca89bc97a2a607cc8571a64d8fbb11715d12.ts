import { BsDropdownModule, BsDropdownConfig } from 'ngx-bootstrap/dropdown';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { DocumentSearchToolbarComponent } from './document-search-toolbar.component';
@NgModule({
  imports: [
    CommonModule,
    BsDropdownModule
  ],
  declarations: [
    DocumentSearchToolbarComponent,
  ],
  exports: [
    DocumentSearchToolbarComponent,
  ],
  providers: []
})

export class DocumentSearchToolbarModule {

}