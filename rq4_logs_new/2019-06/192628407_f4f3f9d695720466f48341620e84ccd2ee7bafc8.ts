import { NgModule } from '@angular/core';
import { PreloadAllModules, RouterModule, Routes } from '@angular/router';


const routes: Routes = [
  { path: '', redirectTo: 'home', pathMatch: 'full' },
  { path: 'home', loadChildren: './home/home.module#HomePageModule' },
  { path: 'sucursales', loadChildren: './componentes/sucursales/sucursales.module#SucursalesPageModule' },
  { path: 'menu', loadChildren: './componentes/menu/menu.module#MenuPageModule' },
  { path: 'ordenar', loadChildren: './componentes/ordenar/ordenar.module#OrdenarPageModule' },
  { path: 'cliente', loadChildren: './componentes/cliente/cliente.module#ClientePageModule' },
  { path: 'menu-orden', loadChildren: './componentes/menu-orden/menu-orden.module#MenuOrdenPageModule' },
  { path: 'cantidad-orden', loadChildren: './componentes/cantidad-orden/cantidad-orden.module#CantidadOrdenPageModule' },
  { path: 'pedido', loadChildren: './componentes/pedido/pedido.module#PedidoPageModule' },
  { path: 'confirmacion', loadChildren: './componentes/confirmacion/confirmacion.module#ConfirmacionPageModule' },
  { path: 'masinfo', loadChildren: './componentes/masinfo/masinfo.module#MasinfoPageModule' },
];

@NgModule({
  imports: [
    RouterModule.forRoot(routes, { preloadingStrategy: PreloadAllModules })
  ],
  exports: [RouterModule]
})
export class AppRoutingModule { }