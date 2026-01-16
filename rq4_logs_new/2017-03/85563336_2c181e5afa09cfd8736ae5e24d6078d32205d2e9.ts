import {Routes} from '@angular/router';
import {CarComponent} from './car/car.component';
import {CarsComponent} from './cars/cars.component';

export const routes: Routes = [
  {
    path: 'car',
    component: CarComponent
  },
  {
    path: 'car/:id',
    component: CarComponent
  },
  {
    path: 'cars',
    component: CarsComponent
  },
  {
    path: '',
    redirectTo: '/cars',
    pathMatch: 'full'
  },
  {
    path: '**',
    redirectTo: '/cars'
  }
];