/***************************************************

CREATED_BY: Yash Saxena

Title:
app (directive)

Purpose:
This is the parent view inside of which the entire app renders

Renders
"/dashboard/templates/parent.html"

Used:
- dashboard app

***************************************************/

/**
 * Import Dependencies
 */
import { Component, View, bootstrap, provide } 		   	   from 'angular2/angular2';

// http
import { HTTP_PROVIDERS }                        		   from 'angular2/http';

// router
import { ROUTER_DIRECTIVES, RouteConfig }       		   from 'angular2/router';
import { Location, ROUTER_PROVIDERS, LocationStrategy }    from 'angular2/router';
import { HashLocationStrategy, Route, AsyncRoute, Router } from 'angular2/router';

// components with views
import { Nav }                                             from '/build/dashboard/scripts/directives/nav.js';
import { Dashboard }                                       from '/build/dashboard/scripts/directives/dashboard.js';
import { Auth }                                            from '/build/dashboard/scripts/directives/auth.js';

/**
 * Decalare Component
 */
@Component({
	selector: 'app',
	templateUrl: "/dashboard/templates/parent.html",
	directives: [Nav, Dashboard, Auth, ROUTER_DIRECTIVES]	
})

/**
 * Configure Routes
 */
@RouteConfig([
	new Route({ path: '/',       component: Auth,       as: 'Auth' }),
	new Route({ path: '/admin',  component: Dashboard,  as: 'Dashboard' })
])

/**
 * Decalare Class
 */
class App {

	// init
	router: Router;
	location: Location;

	constructor(router: Router, location: Location) {
		this.router = router;
		this.location = location;
	}
	
}

/**
 * Bootstrap the app
 */
bootstrap(
	App, 
	[
		ROUTER_PROVIDERS, 
		HTTP_PROVIDERS, 
		provide( LocationStrategy, { 
				useClass: 
				HashLocationStrategy 
		})
	]
);