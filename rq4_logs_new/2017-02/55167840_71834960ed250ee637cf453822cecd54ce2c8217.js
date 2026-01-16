(function(){
	angular
	.module('myApp')
	.controller('UpdateModalController', ['ytInitAPIs', '$uibModalInstance', UpdateModalController])

	function UpdateModalController(ytInitAPIs, $uibModalInstance){
		var vm = this;
		vm.ok = ok;
		vm.cancel = cancel;

		//Existing submissions will occupy their respective boxes
		vm.apisObj = ytInitAPIs.apisObj;
		vm.currentUserName = ytInitAPIs.apisObj.id;

		function ok(obj){
			$uibModalInstance.close(obj);
		}

		function cancel(){
			$uibModalInstance.dismiss();
		}
	}
})();