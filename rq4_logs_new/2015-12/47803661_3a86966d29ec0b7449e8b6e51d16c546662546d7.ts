/// <reference path="../../typings/angular2-meteor.d.ts" />
import {Injectable} from 'angular2/angular2';

@Injectable()
export class Service {
	someProp: boolean;
	hideButton: boolean;
	constructor() {
		this.someProp = false;
		this.hideButton = false;
	}
	
}