import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { RestaurantsComponent } from './components/restaurants/restaurants.component';
import { AuthenticateComponent } from './components/authenticate/authenticate.component';

import { RestaurantItemComponent } from './components/restaurants/restaurant-item/restaurant-item.component';
import { RestaurantDetailsComponent } from './components/restaurants/restaurant-details/restaurant-details.component';
import { RestaurantMenuComponent } from './components/restaurants/restaurant-details/restaurant-menu/restaurant-menu.component';
import { RestaurantCheckoutComponent } from './components/restaurants/restaurant-details/restaurant-checkout/restaurant-checkout.component';
import { FoodItemComponent } from './components/restaurants/restaurant-details/restaurant-menu/food-item/food-item.component';
import { FoodDetailsComponent } from './components/restaurants/restaurant-details/restaurant-menu/food-details/food-details.component';


@NgModule({
  imports: [
    CommonModule

  ],
  declarations: [
    RestaurantsComponent,
    AuthenticateComponent,
    RestaurantItemComponent,
    RestaurantDetailsComponent,
    RestaurantMenuComponent,
    RestaurantCheckoutComponent,
    FoodItemComponent,
    FoodDetailsComponent
  ]
})
export class CoreModule { }