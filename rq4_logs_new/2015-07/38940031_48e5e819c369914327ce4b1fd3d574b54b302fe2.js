import 'es6-shim';
import 'reflect-metadata';
import 'zone.js';

import * as angular from 'angular2/angular2';

import {ThirdApp} from 'app/third-app/app.js';
import {Person} from 'app/second-app/components/person/component.js';
import {PeopleService} from 'app/second-app/services.js';

var secondApp = angular.Component({
	selector: 'second-app',
	appInjector: [PeopleService]
})
.View({
	directives: [angular.NgFor],
	templateUrl: 'app/second-app/template.html'
})
.Class({
	constructor: [PeopleService, function(peopleService){
			
		this.messageList = [
			{
				message: 'Mensajeeeeee'
			},
			{
				message: 'Nada serio'
			},
			{
				message: 'Lo que sea'
			}
		];

		this.peopleService = peopleService;
	
		this.bootstrapThirdApp = function(){
			angular.bootstrap(ThirdApp, [
				// injecto PeopleService como dependencia
				PeopleService
			])
			.then(function(message){
				console.log('third app success', message);
			})
			.catch(function(message){
				console.log('third app error', message);
			});
		};

	}]
});


angular.bootstrap(secondApp, [
	// injecto PeopleService como dependencia
	PeopleService
])
.then(function(message){
	console.log('second app success', message);
})
.catch(function(message){
	console.log('second app error', message);
});