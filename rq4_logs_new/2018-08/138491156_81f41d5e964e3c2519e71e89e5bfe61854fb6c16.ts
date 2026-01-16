import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { RestaurantDetailComponent } from './restaurant-detail.component';

const routes: Routes = [{
  path: 'restaurant/:name',
  component: RestaurantDetailComponent
}];


@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class RestaurantDetailRoutingModule { }