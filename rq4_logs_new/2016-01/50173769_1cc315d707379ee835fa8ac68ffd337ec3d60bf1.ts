import {Injector} from 'angular2/core';
import {appInjector} from './app-injector';
import {AuthService} from './AuthService';
import {Router, ComponentInstruction} from 'angular2/router';
import {Observable} from 'rxjs/Observable';
import 'rxjs/Rx';

export const isLoggedIn = (to: ComponentInstruction, from: ComponentInstruction) => {
	let injector: Injector = appInjector();
	let auth: AuthService = injector.get(AuthService);
	let router: Router = injector.get(Router);

	return new Promise((resolve) => {
		//console.log(auth.check());	
	  auth.check().subscribe((result) => {
		  //console.log('new test', result);
		  
					if (result) {
						resolve(true);
					} else {
						resolve(false);
						router.navigate(['/Home']);
						}
						
				});
			
    });
};