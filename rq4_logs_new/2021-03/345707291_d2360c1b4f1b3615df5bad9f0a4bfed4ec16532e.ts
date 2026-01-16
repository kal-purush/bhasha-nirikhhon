import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import {NotFoundComponent} from '../../not-found/not-found.component';
import {ButtonsComponent} from './buttons.component';
import {RadioButtonComponent} from './radio-button/radio-button.component';


const routes: Routes = [
  {
    path: '', component: ButtonsComponent, children: [

      {
        path: 'radio-button', component: RadioButtonComponent
      },
      {
        path: '', redirectTo: 'radio-button', pathMatch: 'full'
      },
      { path: '**', component:  NotFoundComponent}

    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class ButtonsRoutingModule { }