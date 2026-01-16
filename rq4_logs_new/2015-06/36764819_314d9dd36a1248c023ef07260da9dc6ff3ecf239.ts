/// <reference path="_App.ts" />

module App
{
	Core.MainModule.app
		.service('logger', Common.Logger)
		.service('paymentService', App.Service.PaymentService)
		.controller('ExperimentController', App.Controller.ExperimentController)
		.controller('paymentComponentController', App.Controller.PaymentComponentController)
		.controller('paidbyStoreController', App.Controller.PaidbyStoreController);
	
	//Define directives from the PaymentMethod dictionary
	angular.forEach(App.Data.paymentMethodDictionary, (paymentMethod: App.Model.IPaymentMethodDictionary): void => {
		Core.MainModule.app
			.directive(paymentMethod.directiveSelector, ['logger', (logger: Common.Logger)=> {
			return {
				require: [paymentMethod.directiveSelector],
				controller: paymentMethod.controllerSelector,
				controllerAs: 'componentCtrl',
				templateUrl: './app/form/' + paymentMethod.templatePath,
				scope: {
					description: '@',
					model: '='
				}
			}
		}]);
	});
}