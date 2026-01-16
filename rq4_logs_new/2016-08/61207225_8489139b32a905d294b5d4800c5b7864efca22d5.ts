
import { Routes, RouterModule } from '@angular/router';
import {CarComponent} from "./car.component";
import { CarListComponent } from "./cars-list.component";
import { CarDetailsComponent } from "./car-detail.component";

const routes: Routes = [
  {
    path: 'cars',
    component: CarComponent,
    children: [
      { path: '', component: CarListComponent }
    ]
  }
];

export const routing = RouterModule.forChild(routes);