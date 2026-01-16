import {Component, bootstrap} from 'angular2/angular2';
import {Zippy} from './zippy';

@Component({
	selector: 'my-app',
	template: `
		<zippy title="Angular-2">
			<zippy-details>Angular-2 community is awesome !!!!</zippy-details>
		</zippy>
	`,
	directives: [Zippy]
})
class AppComponent {
	
}

bootstrap(AppComponent);