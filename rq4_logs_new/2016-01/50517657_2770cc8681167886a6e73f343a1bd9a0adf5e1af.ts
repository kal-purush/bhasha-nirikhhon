import {bootstrap} from 'angular2/platform/browser'
import {AppComponent} from './components/app.component'
import {ROUTER_PROVIDERS} from 'angular2/router';
import {Http, HTTP_PROVIDERS} from 'angular2/http';

//use this imports in order to use # url strtegy
//import {provide} from 'angular2/core';
//import {LocationStrategy, HashLocationStrategy} from 'angular2/router';

bootstrap(AppComponent, [ROUTER_PROVIDERS, Http, HTTP_PROVIDERS
	//use this provide in order to use # url strtegy
	//, provide(LocationStrategy, { useClass: HashLocationStrategy }) // .../#/crisis-center/
	]);