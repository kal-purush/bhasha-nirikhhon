angular.module('routerApp', ['routerRoutes'])
  .controller('mainController', function() {
    var vm = this;
    vm.bigMessage = 'Smooth Sea never made Skilled Sailor';
  })
  .controller('homeController', function() {
    var vm = this;
    vm.message = 'This is the homepage';
  })
  .controller('aboutController', function() {
    var vm = this;
    vm.message = 'This is the about page';
  })
  .controller('contactController', function() {
    var vm = this;
    vm.message = 'This is the contact page';
  });