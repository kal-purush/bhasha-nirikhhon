/*
 * Angular
 */
import {Component, Injector} from 'angular2/core';
import {RouteConfig, CanActivate, ROUTER_DIRECTIVES, ROUTER_PROVIDERS, ComponentInstruction} from 'angular2/router';
import {Http, HTTP_PROVIDERS} from 'angular2/http';
import {AuthService, checkToken} from'../services/AuthService';
import {AuthHttp, AuthConfig, JwtHelper, tokenNotExpired} from 'angular2-jwt';
import {Observable} from 'rxjs/Observable';
import {isLoggedIn} from '../services/is-logged-in';
/*
 * Services
 */

@Component({
  selector: 'protected',
  template: `<h1>Protected content</h1>
              <p> this is a secret! </p>
  `,
  directives: [ROUTER_DIRECTIVES]
})
@CanActivate((to: ComponentInstruction, from: ComponentInstruction) => {
  return isLoggedIn(to, from); // navigate to protected route after logging in
})
export class ProtectedComponent {
}