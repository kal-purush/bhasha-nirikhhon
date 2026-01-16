angular.module('starter.controllers', [])

.controller('ProcessesCtrl', function($scope, Processes, $ionicLoading, $timeout) {
  $scope.show = function () {
    $ionicLoading.show({
      duration: 30000,
      noBackdrop: true,
      template: '<p class="item-icon-left">carregando...<ion-spinner icon="lines"/></p>'
    });
  };

  $scope.hide = function(){
    $ionicLoading.hide();
  };

  $scope.show()
  // Método faz request http para a API
  Processes.all()
    .success(function (response) {
      $scope.hide()
      $scope.processes = response;
    })

    .error(function (error) {
      console.log(error);
    });
})