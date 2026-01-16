import { Component, Input, Output, FORM_DIRECTIVES, ControlGroup, Control, Validators, FormBuilder, EventEmitter } from 'angular2/angular2';
import Gasket from '../models/Gasket';

function numeric(control: Control): { [key: string]: any } {
	const value = parseInt(control.value, 10);
	return isNaN(value) ?
		{ 'numeric': { 'value': control.value } } :
		null
	;
}

@Component({
	selector: 'gasket-model-form',
	templateUrl: 'package:app/components/GasketModelForm.html',
	directives: [ FORM_DIRECTIVES ]
})
export default class GasketModelForm {
	@Output() gasketChange = new EventEmitter();

	private _gasket: Gasket;
	private form: ControlGroup;
	private innerDiameter: Control;
	private outerDiameter: Control;

	constructor(fb: FormBuilder) {
		this.form = fb.group({
			outerDiameter: [ '', Validators.compose([
				Validators.required, numeric
			]) ],
			innerDiameter: [ '', Validators.compose([
				Validators.required, numeric
			]) ]
		}, {
			validator: (control: ControlGroup) => {
				const inner = control.find('innerDiameter');
				const outer = control.find('outerDiameter');

				const innerValue = parseInt(inner.value, 10);
				const outerValue = parseInt(outer.value, 10);

				if (inner.valid && outer.valid && innerValue < outerValue) {
					return null;
				}
				return {
					negative: true
				};
			}
		});
		this.innerDiameter = <any> this.form.controls['innerDiameter'];
		this.outerDiameter = <any> this.form.controls['outerDiameter'];
	}

	@Input() private set gasket(value: Gasket) {
		this.innerDiameter.updateValue(value ? value.innerDiameter : '');
		this.outerDiameter.updateValue(value ? value.outerDiameter : '');
		this._gasket = value;
	}

	private get gasket(): Gasket {
		return this._gasket;
	}

	private get disabled(): boolean {
		return !this._gasket;
	}

	private onSubmit() {
		console.log(this.form.value);
	}
}