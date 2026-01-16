import {Component, Input, Output, EventEmitter} from 'angular2/angular2';

@Component({
	selector: 'datenturbo',
	template: `
		<div class="row">
			<div class="col-md-2">
			Datenturbo
			</div>
			<div class="col-md-10">
			<input type="checkbox" [value]="enabled" (change)="onChanged()"></input>
			</div>
      	</div>
	`
})
export class DatenturboComponent {
	@Input('enabled') enabled: Boolean;
	@Output() toggle: EventEmitter = new EventEmitter();
	
	onChanged() {
		this.toggle.next();
	}
}