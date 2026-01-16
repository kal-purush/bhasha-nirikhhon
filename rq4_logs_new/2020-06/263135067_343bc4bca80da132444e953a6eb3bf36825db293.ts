import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { ActualizarProductoPageRoutingModule } from './actualizar-producto-routing.module';

import { ActualizarProductoPage } from './actualizar-producto.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    ActualizarProductoPageRoutingModule
  ],
  declarations: [ActualizarProductoPage]
})
export class ActualizarProductoPageModule {}