import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { DetailBillPageRoutingModule } from './detail-bill-routing.module';

import { DetailBillPage } from './detail-bill.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    DetailBillPageRoutingModule
  ],
  declarations: [DetailBillPage]
})
export class DetailBillPageModule {}