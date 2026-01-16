import { Component, View } from 'angular2/angular2'
import { ROUTER_DIRECTIVES, RouteConfig } from 'angular2/router'
import { PageComponent } from './page-component'

@Component({
  selector: 'app'
})
@View({
  template: `<a [router-link]="['/Page']">Page</a><br>
             <router-outlet></router-outlet>`,
  directives: [ROUTER_DIRECTIVES]
})
@RouteConfig([
  { path: '/page', as: 'Page', component: PageComponent }
])
export class AppComponent {
  name: string
  constructor () {
    this.name = 'Calle'
  }
}