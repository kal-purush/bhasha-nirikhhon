import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { FichaPageRoutingModule } from './ficha-routing.module';

import { FichaPage } from './ficha.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    FichaPageRoutingModule
  ],
  declarations: [FichaPage]
})
export class FichaPageModule {}