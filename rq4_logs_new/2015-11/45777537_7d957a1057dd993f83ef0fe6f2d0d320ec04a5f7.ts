import {Component} from 'angular2/angular2';
import {RouteConfig, ROUTER_DIRECTIVES, Router} from 'angular2/router';
import {Home} from './home';
import {Child} from './child';

@Component({
  selector: 'app',
  template: `
    <a [router-link]="['./Home']">Home</a>
    <button (click)="navigate()">Child</button>
    <router-outlet></router-outlet>
    `,
  directives: [ROUTER_DIRECTIVES]
})
@RouteConfig([
  { path: '/', as: 'Home', component: Home },
  { path: '/child/...', as: 'Child', component: Child }
])
export class App { 
  router : Router;
  
  constructor(router: Router) {
    this.router = router;
  }
  
  navigate() {
    this.router.navigateByUrl('child');
  }  
}