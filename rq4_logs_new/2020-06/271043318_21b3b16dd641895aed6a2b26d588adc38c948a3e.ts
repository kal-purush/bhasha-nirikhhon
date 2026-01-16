import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { TransportSearchPageRoutingModule } from './transport-search-routing.module';

import { TransportSearchPage } from './transport-search.page';

@NgModule({
    imports: [
        CommonModule,
        FormsModule,
        IonicModule,
        TransportSearchPageRoutingModule,
        ReactiveFormsModule
    ],
  declarations: [TransportSearchPage]
})
export class TransportSearchPageModule {}