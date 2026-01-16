import {Component} from 'angular2/core';
import {RouteConfig, ROUTER_DIRECTIVES, ROUTER_PROVIDERS} from 'angular2/router';

import {HomeComponent} from './components/home.component';
import {HeroComponent} from './components/hero.component';

@Component({
  selector: 'my-app',
  template: require('./app.html'),
  directives: [ROUTER_DIRECTIVES],
  providers: [
    ROUTER_PROVIDERS
  ]
})
@RouteConfig([
  {
    path: '/',
    name: 'Home',
    component: HomeComponent,
    useAsDefault: true
  },
  {
    path: '/hero',
    name: 'Hero',
    component: HeroComponent
  }
])
export class AppComponent {
  ngOnInit() {
    console.log('ngOnInit(): AppComponent');
  }
}