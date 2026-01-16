import {Component} from 'angular2/core';
import {RouteConfig, Router, RouterLink, ROUTER_PROVIDERS} from 'angular2/router';
import {Tab1} from './tab1';
import {Tab2} from './tab2';
import {Tab3} from './tab3';
import {ng2RoutingTabs} from "./ng2RoutingTabs/ng2RoutingTabs";
import {ng2RoutingTab} from "./ng2RoutingTabs/ng2RoutingTab";

@Component({
    selector: 'my-app',
    templateUrl: './app/app.html',
    directives: [ng2RoutingTabs, ng2RoutingTab],
    providers: [ROUTER_PROVIDERS]
})
@RouteConfig([
		{ path: '/', component: Tab1, name: 'TabExample1', useAsDefault: true },
		{ path: '/tab2', component: Tab2, name: 'TabExample2' },
		{ path: '/tab3', component: Tab3, name: 'TabExample3' }
])
export class AppComponent { 
	constructor(){

	}
}