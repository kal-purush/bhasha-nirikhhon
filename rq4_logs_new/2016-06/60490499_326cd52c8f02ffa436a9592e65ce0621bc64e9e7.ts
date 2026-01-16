
import { Component } from '@angular/core';
import { ROUTER_DIRECTIVES } from '@angular/router';

@Component({
  selector: 'my-app',
  template: '<h1>My First Angular 2 App</h1><router-outlet></router-outlet>',
  directives: [ROUTER_DIRECTIVES]
})
export class AppComponent { }