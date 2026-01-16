import {Component, NgModule} from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import {HoverComponent} from './hover.component';
import {TestFirstComponent} from './test-first/test-first.component';
import {TestSecondComponent} from './test-second/test-second.component';
import {TestThreeComponent} from './test-three/test-three.component';
import {NotFoundComponent} from '../../not-found/not-found.component';


const routes: Routes = [
  {
    path: '', component: HoverComponent, children: [

      {
        path: 'test-first', component: TestFirstComponent
      },
      {
        path: 'test-second', component: TestSecondComponent
      },
      {
        path: 'test-three', component: TestThreeComponent
      },
      {
        path: '', redirectTo: 'test-first', pathMatch: 'full'
      },
      { path: '**', component:  NotFoundComponent}

    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class HoverRoutingModule { }