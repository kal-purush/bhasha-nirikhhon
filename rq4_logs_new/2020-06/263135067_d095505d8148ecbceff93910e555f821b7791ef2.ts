import { NgModule } from "@angular/core";
import { CommonModule } from "@angular/common";

import { IonicModule } from "@ionic/angular";

import { AgregarTiendaProductoPageRoutingModule } from "./agregar-tienda-producto-routing.module";

import { AgregarTiendaProductoPage } from "./agregar-tienda-producto.page";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    AgregarTiendaProductoPageRoutingModule,
    ReactiveFormsModule,
  ],
  exports: [FormsModule, ReactiveFormsModule],
  declarations: [AgregarTiendaProductoPage],
})
export class AgregarTiendaProductoPageModule {}