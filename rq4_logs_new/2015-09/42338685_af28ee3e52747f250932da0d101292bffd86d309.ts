/// <reference path="./typings/angular2/angular2.d.ts"/>

import {Component, View} from 'angular2/angular2';
import {Http} from 'http/http';

@Component({
    selector: 'friendship',
    injectables: [Http]
})
@View({
    templateUrl: 'friendship.html',
    directives: []
})
export class Friendship {
    constructor(public http: Http) {
    	console.log(this.http)
    }
}