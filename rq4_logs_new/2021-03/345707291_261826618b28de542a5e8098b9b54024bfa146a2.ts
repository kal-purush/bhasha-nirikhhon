import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import {NotFoundComponent} from '../../not-found/not-found.component';
import {TricksComponent} from './tricks.component';
import {ZigZagComponent} from './zig-zag/zig-zag.component';


const routes: Routes = [
  {
    path: '', component: TricksComponent, children: [

      {
        path: 'zig-zag', component: ZigZagComponent
      },
      // {
      //   path: 'test-second', component: TestSecondComponent
      // },
      // {
      //   path: 'test-three', component: TestThreeComponent
      // },
      {
        path: '', redirectTo: 'zig-zag', pathMatch: 'full'
      },
      { path: '**', component:  NotFoundComponent}

    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class TricksRoutingModule { }