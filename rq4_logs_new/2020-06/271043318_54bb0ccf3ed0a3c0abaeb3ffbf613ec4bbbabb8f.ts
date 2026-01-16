import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { PaymentChoicePageRoutingModule } from './payment-choice-routing.module';

import { PaymentChoicePage } from './payment-choice.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    PaymentChoicePageRoutingModule
  ],
  declarations: [PaymentChoicePage]
})
export class PaymentChoicePageModule {}