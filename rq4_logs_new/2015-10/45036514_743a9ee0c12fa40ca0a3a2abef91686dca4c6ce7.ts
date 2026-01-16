import { 
	Component, View, NgIf, NgFor, Injector, 
	EventEmitter, Input, Output, CORE_DIRECTIVES, FORM_DIRECTIVES
} from 'angular2/angular2';
import { CommafyPipe } from './commafy';

@Component({
	selector: "pipes-snippet"
})

@View({
	pipes: [CommafyPipe],
	template: `
		<h2>Pipes</h2>
		<p>Illustration of using pipes.</p>
		
		<h3>Issues</h3>
		<p>
			[Any issues you've had, which may need further investigation]
		</p>
		
		<h3>Resources</h3>
		- <a href="https://angular.io/docs/ts/latest/guide/pipes.html">Pipes on Angular.io</a>
		
		<h3>Snippet</h3>
		<hr/>

		<h4>Examples</h4>	
		<ul>
		</ul>
		<p *ng-for="#example of examples">
			<small>{{example}} === {{example | commafy}}</small>
		</p>
		
	`,
	directives: [CORE_DIRECTIVES, FORM_DIRECTIVES]
})

export class PipesSnippet {
	examples: Array<number>;
	names: Array<string>;
	
	constructor() {	
		
		this.examples = new Array<number>();
		this.examples = [
			-1001,
			-1000,
			-100,
			0,
			100,
			1000,
			1001			
		];
		
		this.names = ["Bob", "Wilma"];	
	}
	
}
