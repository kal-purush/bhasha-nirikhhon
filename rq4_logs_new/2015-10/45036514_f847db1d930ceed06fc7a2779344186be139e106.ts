import { 
	Component, View, NgIf, Injector, 
	EventEmitter, Input, Output
} from 'angular2/angular2';


// ************ BottomComponent **************
@Component({
	selector: "bottom",
	events: ["update"]
})
@View({
	template: `
		<div class="hac box box-bottom">
			<p class="box-label">bottom</p>
		
			age = {{bottomAge}} <button (click)="onFire()">Fire!</button>
		</div>
	`
})

export class BottomComponent {
	@Input() bottomAge: integer = 0;
	@Output() monthlyOutput: EventEmitter = null;
	update: EventEmitter = null;
	
	constructor() {
		//debugger;
		this.monthlyOutput = new EventEmitter();
		this.update = new EventEmitter();
	}
	
	onFire() {
		var monthlyVersion = this.asMonths();
		var decadeVersion = this.asDecades();
		
//		console.log("bottom", monthlyVersion);
		this.update.next(decadeVersion);		
		this.monthlyOutput.next(monthlyVersion);
	}
	
	asMonths() {
		let monthly = (this.bottomAge / 12);
		
		return monthly;
	}
	
	asDecades() {
		let decades = (this.bottomAge / 10);
		
		return decades;
	}
		
}
