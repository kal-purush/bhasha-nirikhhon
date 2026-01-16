import {provideRouter, RouterConfig} from '@angular/router';
import {HomeComponent} from './app/home/home.component';
import {AboutComponent} from './app/about/about.component';
import {DetailsComponent} from './app/details/details.component';

export const routes: RouterConfig = [
  {
    path: '', component: HomeComponent, terminal: true
  },
  {
    path: 'details/:id', component: DetailsComponent
  },
  {
    path: 'about', component: AboutComponent
  }
];

export const APP_ROUTER_PROVIDERS = provideRouter(routes);