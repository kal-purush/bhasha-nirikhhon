import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { BartStationsComponent } from './bart-stations/bart-stations.component';
import { BartStationComponent } from './bart-station/bart-station.component';
import { ScheduleComponent } from './schedule/schedule.component';
const routes: Routes = [
  {path: '', component: BartStationsComponent},
  {path: 'stations', component: BartStationsComponent},
  {path: 'station', component: BartStationComponent},
  {path: 'schedule', component: ScheduleComponent}
];

@NgModule({
imports: [RouterModule.forRoot(routes)],
exports: [RouterModule]
})
export class AppRoutingModule { }