import { 
	Component, View, NgIf, Injector, 
	EventEmitter, Input, Output 
} from 'angular2/angular2';

@Component({
	selector: "component-hierarchy-comms"
})

@View({
	template: `
		<h2>[Snippet Title]</h2>
		<p>
			[Description]
		</p>
		
		<h3>Issues</h3>
		<p>
			[Any issues you've had, which may need further investigation]
		</p>
		
		<h3>Resources</h3>
		<p>
			Any resources you found useful when developing this snippet (e.g. plunks, articles, etc)
		</p>
		
		<h3>Snippet</h3>
		<hr/>
		
		<p>your control snippet</p>
		
		
	`,
	directives: []
})

export class ComponentHierarchyCommsSnippet {
	// @Input() someInput: boolean = false;
	// @Output() someOutput: EventEmitter = new EventEmitter();
	
	constructor() {	
		// To trigger the @Output emitter, use:
		// this.someOutput.next({ some: data });	
	}
	
}
