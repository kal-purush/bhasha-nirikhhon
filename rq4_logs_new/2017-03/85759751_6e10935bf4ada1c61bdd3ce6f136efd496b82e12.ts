import { HomeComponent } from './home.component';
import { EmpresaDetalleComponent } from './../empresa/empresa-detalle.component';
import { EmpresaCrearComponent } from './../empresa/empresa-crear.component';
import { EmpresaListarComponent } from './../empresa/empresa-listar.component';
import { EmpresaComponent } from './../empresa/empresa.component';
import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';


const routes: Routes = [
  {path: '' , component : HomeComponent,
    children: [
        {path : '', loadChildren : 'app/administracion/empresa/empresa.module#EmpresaModule'}
    ]}
];
@NgModule({
  imports: [RouterModule.forChild(routes)], 
  exports: [RouterModule]
})
export class HomeRoutingModule { }