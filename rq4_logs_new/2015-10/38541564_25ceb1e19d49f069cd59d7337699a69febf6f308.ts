import {inject, customAttribute} from 'aurelia-framework';

@customAttribute("validation-hint")
@inject(Element)
export class ValidationHint {
	private value: string;

	constructor(private element: Element) {
	}

	valueChanged(newValue: string) {
		if (newValue) {
			this.element.setAttribute("validate", newValue);
		}
	}
}