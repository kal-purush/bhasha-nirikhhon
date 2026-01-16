import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SharedModule } from '../../app/shared/shared.module';

import { AddressSearchComponent } from './address-search-component/address-search.component';

@NgModule({
  declarations: [
    AddressSearchComponent
  ],
  imports: [
    CommonModule,
  	SharedModule
    ],
    exports: [
      AddressSearchComponent
    ],
    entryComponents:[
      AddressSearchComponent
    ]
})
export class AddressSearchModule {}