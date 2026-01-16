  (function(){

    angular
         .module('app')
         .controller('DocProfileController', [
            'navService', '$mdSidenav', '$mdBottomSheet', '$log', '$q', '$state', 
            DocProfileController
         ]);

    function DocProfileController(navService, $mdSidenav, $mdBottomSheet, $log, $q, $state) {
 
      var vm = this;
      vm.menuItems = [ ];
      vm.selectItem = selectItem;
      vm.toggleItemsList = toggleItemsList;
      vm.title = $state.current.data.title;
 //     vm.showSimpleToast = showSimpleToast;

      navService
        .loadAllItems()
        .then(function(menuItems) {
          vm.menuItems = [].concat(menuItems);
        });

     
      function toggleItemsList() {
        var pending = $mdBottomSheet.hide() || $q.when(true);

        pending.then(function(){
          $mdSidenav('left').toggle();
        });
      }

   
       function selectItem (item) {
        vm.title = item.name;
        vm.toggleItemsList();
      //  vm.showSimpleToast(vm.title);
      }

    }

  })();