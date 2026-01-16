// http://www.geonames.org/
// TODO lagunage selector
// TODO statistics from same api

import { Component } from 'angular2/core';
import { RouteConfig, ROUTER_DIRECTIVES, ROUTER_PROVIDERS } from 'angular2/router';


import {IndexComponent} from './components/index/index';
import {ResultsComponent} from './components/results/results';

@Component({
    selector: 'app',
    template: `
    <router-outlet></router-outlet>
    `,
    directives: [ROUTER_DIRECTIVES],
    providers: [ROUTER_PROVIDERS]
})

@RouteConfig([
    { path: '/', name: 'Index', component: IndexComponent, useAsDefault: true },
    { path: '/search/:name', name: 'Results', component: ResultsComponent }
])

export class AppComponent { }