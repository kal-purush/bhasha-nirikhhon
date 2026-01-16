import { Component, View, FORM_DIRECTIVES, NgIf, Injector, EventEmitter, Input, Output } from 'angular2/angular2';

@Component({
	selector: 'child-control',
	inputs: ['amount'],
	events: ['update']
}

@View({
	directives: [
		FORM_DIRECTIVES
	],
	template: `
		<button (click)="decrement()">-</button>
		{{amount}}
		<button (click)="increment()">+</button>
	`
})

export class ChildControlComponent {
	amount: integer = 0;
	update: Object = null;
	
	constructor() {
		this.update = new EventEmitter();
	}
	
	increment() {
		this.amount++;
		this.recalculateSquare();
	}
	
	decrement() {
		this.amount--;
		this.recalculateSquare();
	}
	
	recalculateSquare() {
		var sq = 0;
		
		sq = (this.amount * this.amount);
		
		// tell caller
		this.update.next(sq);
	}
	
}