import { NgModule }             from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { EventsComponent }      from './events/events.component';
import { EventDetailComponent } from './event-detail/event-detail.component';


const routes: Routes = [
  { path: '', redirectTo: '/events', pathMatch: 'full' },
  { path: 'detail/:address', component: EventDetailComponent },
  { path: 'events', component: EventsComponent }
];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule {}