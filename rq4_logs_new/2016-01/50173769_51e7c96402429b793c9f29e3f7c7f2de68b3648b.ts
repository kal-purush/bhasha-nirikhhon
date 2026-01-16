/**
 * Angular
 */
import {provide, Component, ComponentRef} from 'angular2/core';
import {bootstrap} from 'angular2/platform/browser';
import {FORM_PROVIDERS} from 'angular2/common';

import {
  ROUTER_DIRECTIVES,
  ROUTER_PROVIDERS,
  HashLocationStrategy,
  PathLocationStrategy,
  LocationStrategy,
  Router,
  RouteConfig,
} from 'angular2/router';
import { HTTP_PROVIDERS } from 'angular2/http';
import {UserService} from 'services/UserService';
import {AuthService} from'services/AuthService';
import {AuthHttp, AuthConfig} from 'angular2-jwt';
import {appInjector} from './services/app-injector';
import 'rxjs/Rx';


/**
 * Components
 */
import {LoginComponent} from './components/LoginComponent';
import {HomeComponent} from './components/HomeComponent';
import {AboutComponent} from './components/AboutComponent';
import {ContactComponent} from './components/ContactComponent';
import {ProtectedComponent} from './components/ProtectedComponent';
import {RegisterComponent} from './components/RegisterComponent';

@Component({
  selector: 'ng2-app',
  directives: [ROUTER_DIRECTIVES, LoginComponent],
  template: require('../views/app.html')
})
@RouteConfig([
  { path: '/',          name: 'root',      redirectTo: ['Home'] },
  { path: '/home',      name: 'Home',      component: HomeComponent, useAsDefault: true },
  { path: '/about',     name: 'About',     component: AboutComponent },
  { path: '/contact',   name: 'Contact',   component: ContactComponent },
  { path: '/protected', name: 'Protected', component: ProtectedComponent },
  { path: '/register', name: 'Register', component: RegisterComponent }
  
])
class Ng2App {
  constructor(public router: Router) {
  }
}

bootstrap(Ng2App, [
  ROUTER_PROVIDERS,
  provide(LocationStrategy, {useClass: HashLocationStrategy}),
  provide(AuthConfig, { useFactory: () => {
    return new AuthConfig({
      headerName: 'x-access-token',
      headerPrefix: 'Bearer ',
      noJwtError: true
     })
  }}),
  AuthHttp,
  UserService,
  AuthService,
  FORM_PROVIDERS,
  HTTP_PROVIDERS]).then((appRef: ComponentRef) => {
  // store a reference to the injector
  appInjector(appRef.injector);
});