import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import {NotFoundComponent} from '../../not-found/not-found.component';
import {TextComponent} from './text.component';
import {TextAroundImageComponent} from './text-around-image/text-around-image.component';


const routes: Routes = [
  {
    path: '', component: TextComponent, children: [

      {
        path: 'text-around-image', component: TextAroundImageComponent
      },
      {
        path: '', redirectTo: 'text-around-image', pathMatch: 'full'
      },
      { path: '**', component:  NotFoundComponent}

    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class TextRoutingModule { }