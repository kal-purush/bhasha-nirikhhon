import { Component, Input } from '@angular/core';
import { FormGroup } from '@angular/forms';
import { FormlyFieldConfig, FormlyFormOptions } from '@ngx-formly/core';

@Component({
	selector: 'dynamic-form',
	templateUrl: './dynamic-form.component.html',
	styleUrls: ['./dynamic-form.component.scss']
})
export class DynamicFormComponent {
	@Input() item: any;
	@Input() fields: FormlyFieldConfig[];
	@Input() form: FormGroup;
	@Input() options: FormlyFormOptions;
}