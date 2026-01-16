import { Route } from '@angular/router';
import { ToggleComponent } from './toggle.component';
import { Toogle1Component } from './toogle1/toogle1.component';
import { Toogle2Component } from './toogle2/toogle2.component';
import { Toogle3Component } from './toogle3/toogle3.component';

export const toggleRoues: Route[] = [
  {
    path: '',
    component: ToggleComponent,
  },
  {
    path: 'toggle1',
    component: Toogle1Component,
  },
  {
    path: 'toggle2',
    component: Toogle2Component,
  },
  {
    path: 'toggle3',
    component: Toogle3Component,
  },
];