import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Routes } from '@angular/router';

import { MaindashboardComponent } from './maindashboard/maindashboard.component';
import { EventDetailComponent } from './event-detail/event-detail.component'

const routes: Routes = [
  { path: '', redirectTo: '/maindashboard', pathMatch: 'full' },
  { path: 'maindashboard', component: MaindashboardComponent},
  { path: 'detail/:id', component: EventDetailComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes) ],
  exports: [ RouterModule ],
  declarations: []
})
export class AppRoutingModule { }