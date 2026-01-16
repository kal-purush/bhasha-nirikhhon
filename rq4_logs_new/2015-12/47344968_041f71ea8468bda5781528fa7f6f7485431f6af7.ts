import {Component, NgFor} from "angular2/angular2";

@Component({
	selector: 'stabs',
	templateUrl: 'app/components/stabs/stabs.html',
	directives: [NgFor]
})

export class Stabs {
	tabs: Stab[];
	
	constructor() {
		this.tabs = [];
	}
	
	addTab(tab: Stab) {
		tab.active = this.tabs.length === 0;
		this.tabs.push(tab);
	}
	
	selectTab(tab) {
		this.tabs.forEach((tab) => tab.active = false);
		tab.active = true;
	}		
}

@Component({
	selector: 'stab',
	inputs: [
		'tabTitle: tab-title',
		'active'
	],
	template: `
		<div [hidden]="!active">
			<ng-content></ng-content>
		</div>
	`
})

export class Stab {
	active = this.active || false;
	
	constructor(tabs: Stabs) {
		tabs.addTab(this);	
	}
}