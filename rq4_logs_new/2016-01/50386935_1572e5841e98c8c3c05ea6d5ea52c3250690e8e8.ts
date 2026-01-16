import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES, RouteConfig} from 'angular2/router';
import {ThingsComponent} from './things.component'; 

var markerCount = 0;

@Component({
    selector: 'my-app',
    template: `
    <h1>My App</h1>
    <ul>
    <li><a [routerLink]="['Things', {search: 'foo'}]">Things Search</a>
    <li><a [routerLink]="['Things', 'ThingsDetail', {id: 30}]">Thing 30</a>
    <li><a [routerLink]="['Things', {search: 'foo'}, 'ThingsDetail', {id: 30}]">Thing 30 with search</a>
    </ul>
    <router-outlet></router-outlet>`,
    directives: [ROUTER_DIRECTIVES]
})
@RouteConfig([
    {path: '/things/...', useAsDefault: true, name: 'Things', component: ThingsComponent}
])
export class AppComponent { 
    
    
    
}