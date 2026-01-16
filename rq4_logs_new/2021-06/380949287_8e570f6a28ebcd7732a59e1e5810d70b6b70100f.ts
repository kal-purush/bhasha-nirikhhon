import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { HomeComponent } from '../../modules/home/home.component';
import { AddProductComponent } from '../../modules/product/add-product/add-product.component';
import { DetailsProductComponent } from '../../modules/product/details-product/details-product.component';
import { ProductComponent } from '../../modules/product/index/product.component';
import { DefaultComponent } from './default.component';

const routes: Routes = [
    {
        path: '', component: DefaultComponent,
        children: [
            { path: 'home', component: HomeComponent },
            { path: 'product', component: ProductComponent },
            { path: 'product/add', component: AddProductComponent },
            { path: 'product/:id', component: DetailsProductComponent, data: { animation: 'product' } },
        ]
    }
];

@NgModule({
    imports: [RouterModule.forRoot(routes)],
    exports: [RouterModule]
})
export class DefaultRoutingModule { }