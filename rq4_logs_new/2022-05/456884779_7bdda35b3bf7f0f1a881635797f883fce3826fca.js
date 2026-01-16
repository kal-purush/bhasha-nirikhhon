(function() {
    var PaymentsController = function PaymentsController($scope) {
        var vm = this;
        vm.isFormHidden = true;
        vm.selectedPayment = null;
        vm.called = false;
        vm.newPayment = false;

        vm.handleCancel = function handleCancel() {
            vm.isFormHidden = true;
            vm.selectedPayment = null;
            removeClass();
        };

        vm.handleNewPayment = function handleNewPayment() {
            vm.isFormHidden = false;
            vm.newPayment = true;
        };
        
        vm.handleDelete = function handleDelete() {
            var status = confirm("Are you sure you want to delete the item?");
            if (status) {
                vm.payments = vm.payments.filter(function(obj) {
                    return obj.id !== vm.selectedPayment.id;
                });
                vm.isFormHidden = true;
                vm.selectedPayment = null;
            }
        };
        
        vm.handleSave = function handleSave(isFormValid) {
            if (isFormValid) {
                if (vm.newPayment) {
                    var ids = vm.payments.map(function(obj) {
                        return obj.id
                    });
                    
                    var maxId = Math.max.apply(Math, ids)
                    vm.selectedPayment.id = ++maxId;
                    vm.payments.push(vm.selectedPayment);
                }
                else {
                    var idx = vm.payments.findIndex(function(obj) {
                        return obj.id == vm.selectedPayment.id
                    })
                    
                    if (idx > -1) {
                        vm.payments[idx] = angular.copy(vm.selectedPayment);
                    }
                }
            }
            else {
                vm.formError = true;
            }
        };
        
        vm.payments = [
            {
                id: 1,
                description: "Pencil",
                quantity: "2",
                amount: "20"
            }, 
            {
                id: 2,
                description: "Sharpener",
                quantity: "1",
                amount: "15",
            }, 
            {
                id: 3,
                description: "Pen",
                quantity: "2",
                amount: "50",
            }
        ];
    };
  
    var HighLightDirective = function() {
        return {
            restrict: 'AE',
            scope: false,
            controller: 'PaymentsController',
            controllerAs: 'vm',
            link: function($scope, elem, attr) {
                elem.bind("click", function() {
                    event.preventDefault();
                    var selectedItem = JSON.parse(attr.item);
                    $scope.$apply(function() {
                        $scope.$parent.vm.selectedPayment = selectedItem;
                        $scope.$parent.vm.isFormHidden = false;
                        $scope.$parent.vm.newPayment = false
                    });
                });
            }
        }
    }
  
    angular.module("paymentsApp", [])
      .controller("PaymentsController", PaymentsController)
      .directive('highLight', HighLightDirective)
    
    angular.module('mypfApp', []);

}())