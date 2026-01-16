import 'angular';
import 'angular-router';
import {app} from 'app';

// explicit imports
import 'layout/main.controller';
import 'login/login.controller';

class AppController {

	static $routeConfig = [
		{ path: '/', component: 'main' },
		{ path: '/login', component: 'login' }
	];

}

// register controller with app
app.controller('AppController', AppController);