import { NgModule } from '@angular/core';
import {  RouterModule, Routes } from '@angular/router';
import { StoreComponent } from './store/store.component';
import { CartDetailComponent } from './store/cart-detail.component';
import { ChechoutComponent } from './store/chechout.component';
import { StoreFirstGuard } from './store-first.guard';

const routes: Routes = [
      {path: 'store', component: StoreComponent, canActivate: [StoreFirstGuard]},
      {path: 'cart', component: CartDetailComponent, canActivate: [StoreFirstGuard]},
      {path: 'checkout', component: ChechoutComponent, canActivate: [StoreFirstGuard]},
      {path: '**', redirectTo: '/store'},
    ];

@NgModule({
  imports: [
    RouterModule.forRoot(routes)
  ],
  exports: [RouterModule],
  providers: [StoreFirstGuard],
  declarations: []
})
export class AppRoutingModule { }