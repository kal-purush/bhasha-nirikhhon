import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Routes, RouterModule } from '@angular/router';

import { IonicModule } from '@ionic/angular';

import { AvailabilityMgrPage } from './availability-mgr.page';

const routes: Routes = [
  {
    path: '',
    component: AvailabilityMgrPage
  }
];

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    RouterModule.forChild(routes)
  ],
  declarations: [AvailabilityMgrPage]
})
export class AvailabilityMgrPageModule {}