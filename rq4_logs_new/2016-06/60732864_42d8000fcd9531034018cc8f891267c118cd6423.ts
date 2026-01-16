import { Component } from '@angular/core';
import { AsyncRoute, RouteConfig,
         ROUTER_DIRECTIVES, ROUTER_PROVIDERS } from '@angular/router-deprecated';

import { HeroService } from './common';

@Component({
  template: require('./app.component.html'),
  styles: [require('./app.component.scss')],
  selector: 'ng2-playground',
  directives: [ROUTER_DIRECTIVES],
  providers: [ROUTER_PROVIDERS, HeroService],
})
@RouteConfig([
  new AsyncRoute({
    path: '/dashboard',
    name: 'Dashboard',
    loader: () => System.import('./dashboard').then((c: any) => c.DashboardComponent),
    useAsDefault: true,
  }),
  new AsyncRoute({
    path: '/heroes',
    name: 'Heroes',
    loader: () => System.import('./heroes').then((c: any) => c.HeroesComponent),
  }),
  new AsyncRoute({
    path: '/detail/:id',
    name: 'HeroDetail',
    loader: () => System.import('./heroes').then((c) => c.HeroDetailComponent),
  }),
])
export class AppComponent {
  title = 'Tour of Heroes';
}