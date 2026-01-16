import { NgModule }     from '@angular/core';
import { Routes,
         RouterModule } from '@angular/router';

import { DroprobotComponent }       from './droprobot.component';

const routes: Routes = [
  { path: '',
    component: DroprobotComponent,
    children: [
      { path: '',    component: DroprobotComponent, data: {theme:"default"} }
    ]
  },
  { path: 'droprobot',
    component: DroprobotComponent,
    children: [
      { path: '',    component: DroprobotComponent, data: {theme:"default"} }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class DroprobotRoutingModule {}