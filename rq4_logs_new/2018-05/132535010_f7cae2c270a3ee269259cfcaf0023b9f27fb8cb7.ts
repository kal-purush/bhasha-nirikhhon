import { NgModule } from '@angular/core';

import { RouterModule, Routes } from '@angular/router';
import { GrassrootsHomeComponent } from './grassroots-home/grassroots-home.component';
import { GrassrootsAboutComponent } from './grassroots-about/grassroots-about.component';
import { GrassrootsProductsComponent } from './grassroots-products/grassroots-products.component';

const routes: Routes = [
  { path: '', redirectTo: '/home', pathMatch: 'full' },
  { path: 'home', component: GrassrootsHomeComponent },
  { path: 'about', component: GrassrootsAboutComponent },
  { path: 'products', component: GrassrootsProductsComponent }
]

@NgModule({
  imports: [
    RouterModule.forRoot(routes)
  ],
  exports: [
    RouterModule
  ],
  declarations: [],

})
export class AppRoutingModule { }