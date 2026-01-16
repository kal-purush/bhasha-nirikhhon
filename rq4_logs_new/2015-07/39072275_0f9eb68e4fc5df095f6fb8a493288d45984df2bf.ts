module mallConsoleApp {
  'use strict';
  interface loginScope extends ng.IScope {
    loginData: Object;
    message: Object;
    login: Function;
  }
  export class LoginController {

    constructor($scope: loginScope, $location: ng.ILocationService, authService: any) {
      $scope.loginData = {
        userName: "",
        password: ""
        //useRefreshTokens: true,不用刷新令牌
      };
      $scope.message = "";
      $scope.login = function () {

        authService.login($scope.loginData).then(function (response) {
            $location.path('/');
          },
          function (err) {
            $scope.message = err.error_description;
          });
      };

    }

  }
}