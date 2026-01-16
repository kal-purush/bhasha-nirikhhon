import {Component, View, bootstrap, NgFor} from 'angular2/angular2';

@Component({
	selector: 'display'
})

@View({
	templateUrl: 'app/templates/display.html',
	directives: [NgFor]
})

export class DisplayComponent {
	myName: string;
	names: Array<string>;

	constructor() {
		this.myName = 'Neetu';
		this.names = ['Ankur', 'Yuvaan', 'Kalash', 'Priyanka', 'Ratna', 'Om'];
	}
}