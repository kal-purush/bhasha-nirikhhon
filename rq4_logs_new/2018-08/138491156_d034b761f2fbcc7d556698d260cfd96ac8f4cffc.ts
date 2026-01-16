import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RestaurantsComponent } from './restaurants.component';


import { RestaurantItemComponent } from './restaurant-item/restaurant-item.component';
import { RestaurantDetailsComponent } from './restaurant-details/restaurant-details.component';
import { RestaurantMenuComponent } from './restaurant-details/restaurant-menu/restaurant-menu.component';
import { RestaurantCheckoutComponent } from './restaurant-details/restaurant-checkout/restaurant-checkout.component';
import { FoodItemComponent } from './restaurant-details/restaurant-menu/food-item/food-item.component';
import { FoodDetailsComponent } from './restaurant-details/restaurant-menu/food-details/food-details.component';


@NgModule({
  imports: [
    CommonModule
  ],
  declarations: [
    RestaurantsComponent,
    RestaurantItemComponent,
    RestaurantDetailsComponent,
    RestaurantMenuComponent,
    RestaurantCheckoutComponent,
    FoodItemComponent,
    FoodDetailsComponent
  ],
  exports: [
    RestaurantsComponent
  ]
})
export class RestaurantsModule { }