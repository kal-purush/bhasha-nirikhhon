module Smoothies.Controllers {
  export class SmoothiesController {

      constructor(
        private $scope,
        private $http:angular.IHttpService,
        private $routeParams
      ){
        $http.get('smoothies/' + $routeParams.smoothies_id + '.json').success(function(data) {
          $scope.data = data;
        });
      }
  }

  angular.
    module("Smoothies").
    controller("Smoothies.Controllers.SmoothiesController", SmoothiesController)
}