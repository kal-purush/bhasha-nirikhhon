'use strict';
import * as ng from 'angular';

import 'normalize.css';
import 'angular-material/angular-material.css!';
import 'angular-material';

import registerMySideNav from 'my-side-nav/my-side-nav';
import registerMyApp from 'my-app/my-app';


let myAppModule = ng.module('myApp', ['ngMaterial']);

registerMySideNav(myAppModule);
registerMyApp(myAppModule);


// bootstrap angular manually on whole document
ng.element(document).ready(function(): void {
	ng.bootstrap(document, ['myApp']);
});
