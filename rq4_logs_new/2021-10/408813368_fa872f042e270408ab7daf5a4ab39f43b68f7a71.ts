import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { EngagementModule } from './engagement.module';
import { RouterModule, Routes } from '@angular/router';
import { EngagementPageComponent } from './engagement-page.component';

const routes: Routes = [
  {
    path: '',
    component: EngagementPageComponent,
  },
];

@NgModule({
  declarations: [],
  imports: [CommonModule, RouterModule.forChild(routes), EngagementModule],
})
export class EngagementRoutingModule {}