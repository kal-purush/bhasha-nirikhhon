import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { HuntingComponent } from './hunting.component';

const routes: Routes = [{ path: '', component: HuntingComponent }];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class HuntingRoutingModule { }