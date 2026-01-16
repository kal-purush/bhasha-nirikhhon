import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { InitializationPageRoutingModule } from './initialization-routing.module';

import { InitializationPage } from './initialization.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    InitializationPageRoutingModule
  ],
  declarations: [InitializationPage]
})
export class InitializationPageModule {}