(function() {
    'use strict';

    angular
        .module('xyz-cv-ui.profile.modal')
        .controller('AssignmentsController', AssignmentsController);

        function AssignmentsController(AssignmentsModal, block, user, callback) {
            var vm = this;
            window.vm2 = vm;

            vm.user = {};
            vm.assignments = block;
            vm.hideModal = AssignmentsModal.deactivate;
            vm.save = save;

            activate();

            function activate() {
                Users.get(user).$promise
                    .then(function(user) {
                        vm.user = user;
                    })
            }

            function save() {
                //vm.user = angular.extend(vm.user, vm.assignments);

                vm.user.$save()
                    .then(vm.hideModal)
                    .then(callback);
            }

            function refresh() {
                /* */
            }
        }
})();