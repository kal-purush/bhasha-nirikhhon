import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Routes, RouterModule } from '@angular/router';

import { IonicModule } from '@ionic/angular';

import { ProductPagePage } from './product-page.page';

const routes: Routes = [
  {
    path: '',
    component: ProductPagePage,
    children: [
      {
        path: ':id',
        loadChildren: './product-page.module#ProductPagePageModule'
      }
    ]
  }
];

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    RouterModule.forChild(routes)
  ],
  declarations: [ProductPagePage]
})
export class ProductPagePageModule {}