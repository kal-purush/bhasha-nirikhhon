import { Component, Host, Inject, forwardRef } from 'angular2/angular2';
import { RouterOutlet, RouterLink, RouteConfig } from 'angular2/router';

@Component({
	selector: 'gasket-name',
	template: `
	<label>Name</label> <input type="text">
	<button type="button" [router-link]="['../GasketDimensions']">Next</button>
	`,
	directives: [ RouterLink ]
})
export class GasketName {
	private creator: GasketCreator;
	constructor(@Host() @Inject(forwardRef(() => GasketCreator)) creator: GasketCreator) {
		this.creator = creator;
	}

	onActivate() {
		this.creator.progress = 50;
	}
}

@Component({
	selector: 'gasket-dimensions',
	template: `
	<div>Dimensions</div>
	<button type="button" [router-link]="['../GasketName']">Back</button>
	`,
	directives: [ RouterLink ]
})
export class GasketDimensions {
	private creator: GasketCreator;
	constructor(@Host() @Inject(forwardRef(() => GasketCreator)) creator: GasketCreator) {
		this.creator = creator;
	}

	onActivate() {
		this.creator.progress = 100;
	}
}

@Component({
	selector: 'gasket-creator',
	templateUrl: 'package:app/views/GasketCreator.html',
	directives: [ RouterOutlet ]
})
@RouteConfig([
	{ path: '/', redirectTo: '/name' },
	{ path: '/name', component: GasketName, as: 'GasketName' },
	{ path: '/dimensions', component: GasketDimensions, as: 'GasketDimensions' }
])
export default class GasketCreator {
	public progress: number = 0;

	canReuse() {
		return true;
	}
}