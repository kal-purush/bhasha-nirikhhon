import { NgModule }             from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { DashboardComponent }   from './dashboard/dashboard.component';
import { EventDetailComponent } from './event-detail/event-detail.component';
import { ActivityCategoryDetailComponent } from './activity-category-detail/activity-category-detail.component';
import { EventComponent } from './event/event.component';
import { ActivityCategoryComponent } from './activity-category/activity-category.component';


const routes: Routes = [
  { path: '', redirectTo: '/dashboard', pathMatch: 'full' },
  { path: 'dashboard',  component: DashboardComponent, pathMatch: 'full' },
  { path: 'detail/:id', component: EventDetailComponent, pathMatch: 'full' },
  { path: 'events',     component: EventComponent }
];
@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule {}