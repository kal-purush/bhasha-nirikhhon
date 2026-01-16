import {Component, OnInit} from 'angular2/core';
import {ROUTER_DIRECTIVES} from 'angular2/router';
import{RouteConfig} from 'angular2/router';
import {WelcomeScreenComponent} from './welcome-screen.component';
import {LoginComponent} from './login.component';
import {HistoryComponent} from './history.component';


@Component({
	selector: 'my-app',
	template: `
		<h1>Prijava ispita early days!</h1>
		<header>
			<nav>
				<a [routerLink] = "['Login']" style ="float:right">Login</a>
				<a [routerLink] = "['Welcome']">Main Screen</a>
				<a [routerLink] = "['History']">History</a>
			</nav>
		</header>
		<div>
			
			<router-outlet></router-outlet>
		</div>
		`,
	directives: [ROUTER_DIRECTIVES]
})

@RouteConfig([
	{path :'/welcome', name: 'Welcome', component: WelcomeScreenComponent, useAsDefault: true},
	{path :'/login', name: 'Login', component: LoginComponent},
	{path : '/History', name: 'History', component: HistoryComponent}
])
export class AppComponent
{

 }

/*
Copyright 2016 Google Inc. All Rights Reserved.
Use of this source code is governed by an MIT-style license that
can be found in the LICENSE file at http://angular.io/license
*/