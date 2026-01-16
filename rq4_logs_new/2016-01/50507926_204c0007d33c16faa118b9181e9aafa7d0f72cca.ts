// Angular
import { Component, View } from 'angular2/core';
import { Router, RouterLink } from 'angular2/router';
import { CORE_DIRECTIVES, FORM_DIRECTIVES } from 'angular2/common';


import { ActionItemsService } from '../services/action-items-service';

let allTemplate = require('./all.html');

@Component({
    selector: 'action-items-all',
    providers: [ActionItemsService]
})

@View({
  directives: [ RouterLink, CORE_DIRECTIVES, FORM_DIRECTIVES ],
  template: allTemplate
})

export class ActionItemsAll{
	arrjsonData:any;

	constructor( public actionItemsService: ActionItemsService ) {
		actionItemsService.getReport()
		.map(r => r.json())
		.subscribe( response => {
		    this.arrjsonData = response.expenses;
		});
		
	}
}