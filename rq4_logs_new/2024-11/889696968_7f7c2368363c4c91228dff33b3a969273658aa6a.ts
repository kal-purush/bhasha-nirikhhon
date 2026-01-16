import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { CreateBusPageRoutingModule } from './create-bus-routing.module';

import { CreateBusPage } from './create-bus.page';
import { SharedModule } from 'src/app/shared/shared.module';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    CreateBusPageRoutingModule,
    SharedModule
  ],
  declarations: [CreateBusPage]
})
export class CreateBusPageModule {}