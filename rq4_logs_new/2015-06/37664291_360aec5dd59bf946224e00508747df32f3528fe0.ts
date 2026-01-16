module util {
	'use strict';

	export class KeyEnterDirective implements angular.IDirective {

		public link;

		restrict = 'A';
		scope = false;

		constructor() {
			this.link = this.unboundLink.bind(this);
		}

		unboundLink(scope: angular.IScope, element: JQuery, attributes: any) {
			element.bind('keydown keypress', function(event: JQueryEventObject) {
				if (event.which === 13) {
					scope.$apply(function () {
						scope.$eval(attributes.keyEnter);
					});

					event.preventDefault();
				}
			});
		}

		static instance(): ng.IDirectiveFactory {

			var directive = () => new KeyEnterDirective();
			directive.$inject = [];
			return directive;
		}

	}
}