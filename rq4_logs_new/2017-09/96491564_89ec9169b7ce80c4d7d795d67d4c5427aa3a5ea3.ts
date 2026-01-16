import { NgModule } from '@angular/core';

import {
  RouterModule,
  Routes
} from '@angular/router';

import { HomeComponent } from './home/home.component';
import { SearchComponent } from './search/search.component';


const appRoutes: Routes = [
  {
    path: 'search',
    component: SearchComponent
  },
  {
    path: '',
    component: HomeComponent
  },
  {
    path: '**',
    redirectTo: ''
  }
];

@NgModule({
   exports: [
      RouterModule
   ],
   imports: [
      RouterModule.forRoot(appRoutes, { useHash: true })
   ]
})

export class AppRouter { }