import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { OlvidacontrasePageRoutingModule } from './olvidacontrase-routing.module';

import { OlvidacontrasePage } from './olvidacontrase.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    OlvidacontrasePageRoutingModule
  ],
  declarations: [OlvidacontrasePage]
})
export class OlvidacontrasePageModule {}