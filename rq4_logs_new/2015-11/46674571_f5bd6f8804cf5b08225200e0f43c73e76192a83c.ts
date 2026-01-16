/// <reference path="../../../typings/angular2/angular2.d.ts" />

import { Component, NgFor } from 'angular2/angular2';

@Component({
	selector: 'about',
	directives: [NgFor],
	template: `
		<p>{{title}}</p>
		<ul>
			<li *ng-for="#item of list">{{item.name}}</li>
		</ul>
	`
})

class About {
	title: string;
	list: Array<Object>;
	
	constructor() {
		this.title = 'I am the about component!';
		this.list = [
			{ name: 'List item one' },
			{ name: 'List item two' },
			{ name: 'List item three' }
		];
	}	
};

export default About;