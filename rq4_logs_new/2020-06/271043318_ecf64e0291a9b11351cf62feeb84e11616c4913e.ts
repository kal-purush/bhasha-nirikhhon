import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { ModalDeliveryPageRoutingModule } from './modal-delivery-routing.module';

import { ModalDeliveryPage } from './modal-delivery.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    ModalDeliveryPageRoutingModule
  ],
  declarations: [ModalDeliveryPage]
})
export class ModalDeliveryPageModule {}