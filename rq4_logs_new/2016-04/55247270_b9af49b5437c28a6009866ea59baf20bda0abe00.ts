import {Component, Directive, ElementRef, Renderer} from 'angular2/core';
import {RouteConfig, ROUTER_DIRECTIVES} from 'angular2/router';

import { Test } from './features/test/test';
import { XLarge } from './xlarge';
import { Home } from './features/home/home';
import { About } from './features/about/about';

@Component({
  selector: 'app',
  directives: [
    ...ROUTER_DIRECTIVES,
    XLarge
  ],
  styles: [`
    .router-link-active {
      background-color: lightgray;
    }
  `],
  template: `
  <div>
    <nav>
      <a [routerLink]=" ['./Home'] ">Home</a>
      <a [routerLink]=" ['./About'] ">About</a>
      <a [routerLink]=" ['./Test'] ">test</a>
    </nav>
    <div>
      <span x-large>Hello, {{ name }}!</span>
    </div>

    name: <input type="text" [value]="name" (input)="name = $event.target.value" autofocus>
    <main style="border: solid 1px gray; padding: 10px; border-radius:5px; margin-top:12px; width: 50%; min-height: 200px">
      <router-outlet></router-outlet>
    </main>
  </div>
  `
})
@RouteConfig([
  { path: '/', component: Home, name: 'Home', useAsDefault: true },
  { path: '/home', component: Home, name: 'Home' },
  { path: '/about', component: About, name: 'About' },
  { path: '/test', component: Test, name: 'Test' },
  { path: '/**', redirectTo: ['Home'] }
])
export class App {
  name: string = 'Angular 2';
}

