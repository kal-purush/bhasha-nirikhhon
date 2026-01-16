/// <reference path="../../../typings/angularjs/angular.d.ts" />
/// <reference path="../../../typings/angularjs/angular-route.d.ts" />

module App {
  export module Ctrl {
    export class Test {
      static $inject = ['$scope'];
      constructor(
        private $scope: ng.IScope) {
          $scope.$watch('size', (data:string)=>{
            let size = parseInt(data);
            if (!size) {
              $scope['size'] = '0';
              size = 0;
            }
            let a = [];
            for (let i = 0; i < size; ++i) {
              a.push(i);
            }
            $scope['array'] = a;
          });
      }
    }
  }
}
