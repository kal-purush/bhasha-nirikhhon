import { Routes }  from '@angular/router';

//components
import { AppComponent }             from './app.component';

// Definition of routes
export const appRoutes : Routes = [
  {
    path : '/todolist',
    component: AppComponent  //TODO : use appropriated page
  },
  {
    path : '/player',
    component: AppComponent //TODO : use appropriated page
  },
  {
    path : '/calculator',
    component: AppComponent //TODO : use appropriated page
  },
  {
    path     : '**',
    redirectTo: ''
  }

];