module Komponent {
	
	function getPathValue<T>(root: any, path: string) {
        return (new Function("$context", `with($context){return ${path}}`))(root);
	}

	export class Abstract {

		constructor(params: any, info: KnockoutComponentTypes.ComponentInfo) {
		}

		protected static parameters(info: KnockoutComponentTypes.ComponentInfo) {
			let result = <Base.Parameter>{};
			let data = ko.dataFor(info.element);

			if (info.element.nodeType === Node.ELEMENT_NODE) {
				let element = <HTMLElement>info.element;
				for (let i = 0; i < element.attributes.length; i++) {
					let name = element.attributes[i].name;
					let value = element.attributes[i].value;
					let match = /^\${([^}]*)}$/g.exec(value);
					result[name] = match ? getPathValue(data, match[1]) : value;
				}
			}

			return result;
		}

		static register(alias: string, ViewModel: typeof Abstract, template: string) {
			let registration = <KnockoutComponentTypes.Config>{};

			registration.template = template || " ";
			if (ViewModel) {
				registration.viewModel = {
					createViewModel: (params: any, componentInfo: KnockoutComponentTypes.ComponentInfo) => {
						return new ViewModel(params, componentInfo);
					}
				};
			}

			(<any>ko.components).register(`ko-${alias}`, registration);
		}
	}

	export module Base {
		export interface Parameter {
			[name: string]: any | KnockoutObservable<any>;
		}
	}
}