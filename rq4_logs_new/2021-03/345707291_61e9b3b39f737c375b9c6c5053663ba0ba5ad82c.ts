import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import {NotFoundComponent} from '../../not-found/not-found.component';
import {JsComponent} from './js.component';
import {RandomColorGeneratorComponent} from './random-color-generator/random-color-generator.component';


const routes: Routes = [
  {
    path: '', component: JsComponent, children: [
      {
        path: 'random-color-generator', component: RandomColorGeneratorComponent,
      },

      {
        path: '', redirectTo: 'random-color-generator', pathMatch: 'full'
      },

      { path: '**', component: NotFoundComponent  }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class JsRoutingModule { }