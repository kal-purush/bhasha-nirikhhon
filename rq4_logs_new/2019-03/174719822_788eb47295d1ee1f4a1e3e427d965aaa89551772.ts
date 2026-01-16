import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { MainComponent } from './components/main/main.component';
import { NegotiumTaskComponent } from './components/negotium-task/negotium-task.component';

const routes: Routes = [
  { path: '', component: MainComponent },
  { path: 'negotium-task', component: NegotiumTaskComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {}