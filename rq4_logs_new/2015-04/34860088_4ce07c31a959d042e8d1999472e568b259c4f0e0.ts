module Komponents {
	
	export class Button extends Abstract {
		
		type: KnockoutObservable<Button.Type>;
		
		constructor(params: any, info: KnockoutComponentTypes.ComponentInfo) {
			super(params, info);
			let parameters = Button.parameters(info);
			if (parameters["type"]) {
				this.type = <KnockoutObservable<Button.Type>>(
					ko.isObservable(parameters["type"])?
						parameters["type"]
						:
						ko.observable(
							Utils.toEnum<Button.Type>(parameters["type"], Button.Type)
						)
					);
			} else {
				this.type = ko.observable(Button.Type.Default);
			}
		}
	}
	
	export module Button {
		export enum Type {
			Default,
			Primary,
			Success,
			Info,
			Warning,
			Danger,
			Link
		}
	}
}