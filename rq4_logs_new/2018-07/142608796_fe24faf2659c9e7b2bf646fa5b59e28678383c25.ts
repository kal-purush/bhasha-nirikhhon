import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import {AppComponent} from './app.component';

const routes: Routes = [
  { path: '', redirectTo: '/userList', pathMatch: 'full' },
  { path: 'userList', component: AppComponent },
  { path: 'daily', component: AppComponent },
  { path: 'brainstorm', component: AppComponent },
  { path: 'course', component: AppComponent },
  { path: 'levelingSystem', component: AppComponent },
  { path: 'liveStreams', component: AppComponent },
  { path: 'userList', component: AppComponent },
  { path: 'videoAnalytics', component: AppComponent },
  { path: 'advanced', component: AppComponent },
];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule {}
