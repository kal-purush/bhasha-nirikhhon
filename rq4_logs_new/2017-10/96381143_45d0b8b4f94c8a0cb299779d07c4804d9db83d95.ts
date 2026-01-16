import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ModalModule } from 'ngx-modal';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { MyDateRangePickerModule } from 'mydaterangepicker';

import { DateFilterComponent } from './date-filter.component';

@NgModule({
  imports: [
    CommonModule,
    ModalModule,
    BsDropdownModule,
    MyDateRangePickerModule
  ],
  declarations: [
    DateFilterComponent,
  ],
  exports: [
    DateFilterComponent,
  ],
  providers: [

  ]
})

export class DateFilterModule {

}