(function(){
	'use strict';
	angular.module('app').controller('appController', appController);

	appController.$inject = ['$scope', 'ajaxService', 'calculatorService'];

	function appController($scope, ajaxService, calculatorService) {
		var vm = this;
		vm.costs = [];
		vm.inputs = {
			length: null,
			breadth: null,
			height: null,
			weight: 1
		};
		vm.validPackage = {};
		vm.idealResults = {};
		vm.maxWeight = 1;
		vm.validWeight = false;

		$scope.$watch('vm.inputs', _pickPackage, true);

    _activate();

		/*private functions*/
		function _activate(){
			return ajaxService.getCosts().then(function(response){
				if(response && response.data && response.data.costs){
					vm.costs = response.data.costs;
					vm.maxWeight = response.data.maxWeight;
				}
			});
		}

		function _pickPackage(){
			if(!vm.inputs.weight || vm.inputs.weight < 1 || vm.inputs.weight > vm.maxWeight){
				vm.validPackage = {};
				vm.idealResults = {};
				vm.validWeight = false;
				return false;
			}
			vm.validWeight = true;
			vm.validPackage = calculatorService.pickPackage(vm.costs, vm.inputs);
			vm.idealResults = calculatorService.pickIdealResults(vm.costs, vm.inputs);
		}
		/*end private functions*/

		/*public functions*/
		/*end public functions*/
	}
})();