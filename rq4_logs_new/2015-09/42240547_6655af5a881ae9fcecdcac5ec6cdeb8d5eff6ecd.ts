import {Component, View, bootstrap} from 'angular2/angular2';
import {RouteConfig, ROUTER_DIRECTIVES, ROUTER_BINDINGS} from 'angular2/router';

import{ Http, HTTP_BINDINGS} from 'http/http';
import {Custom} from './components/custom/custom';


@Component({
  selector: 'app',
  viewBindings: [Http, HTTP_BINDINGS]
  
})
@RouteConfig([
  { path: '/custom', component: Custom, as: 'custom' }
])
@View({
  templateUrl: './app.html?v=<%= VERSION %>',
  directives: [ROUTER_DIRECTIVES]
})
class App {}

bootstrap(App, [ROUTER_BINDINGS]);