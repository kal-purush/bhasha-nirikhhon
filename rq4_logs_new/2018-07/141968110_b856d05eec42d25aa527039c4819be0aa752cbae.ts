import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import {HomeComponent} from '../home/home.component';

const routes: Routes = [
  {
    path: 'home',
    loadChildren: '../home/home.module#HomeModule'
  },
  {
    path: 'login',
    loadChildren: '../core/core.module#CoreModule'
  },
  {
    path: 'registration',
    loadChildren: '../core/core.module#CoreModule'
  },
  {
    path: '',
    component: HomeComponent
  },
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})

export class SharedRoutingModule {}