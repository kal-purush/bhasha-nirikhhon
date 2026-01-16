import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ModuloUsuariosComponent } from './modulo-usuarios/modulo-usuarios.component';
import { ModuloVehiculosComponent } from './modulo-vehiculos/modulo-vehiculos.component';
import { ModuloServiciosComponent } from './modulo-servicios/modulo-servicios.component';



@NgModule({
  declarations: [
    ModuloUsuariosComponent,
    ModuloVehiculosComponent,
    ModuloServiciosComponent
  ],
  imports: [
    CommonModule
  ]
})
export class AyudaModule { }