/**
 * Created by domin on 23.05.2017.
 */

import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { EquipmentsComponent } from './pages/equipments/equipments.component';
import { EquipmentDetailComponent } from './pages/equipment-detail/equipment-detail.component';
import { StartComponent} from './pages/start/start.component';
import { CarouselComponent} from './pages/carousel/carousel.component';

const routes: Routes = [
  { path: '', redirectTo: '/equipments', pathMatch: 'full' },
  { path: 'detail/:id', component: EquipmentDetailComponent },
  { path: 'equipments', component: EquipmentsComponent },
  { path: 'start', component: CarouselComponent },
];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule {}