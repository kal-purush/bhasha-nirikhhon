(function() {
  'use strict';
  var app = angular.module('donup');
  app.controller('ConfirmEmailLinkCtrl', [ '$scope', '$location', '$routeParams', '$route','UserService',
      function($scope, $location, $routeParams, $route, userService) {
      $scope.user={};
      $scope.user.confirmEmailURLLink=$routeParams.param;
        confirmEmailLink();
        function confirmEmailLink() {
          userService.confirmEmailLink($scope.user).success(function(data) {
           if(data.returnCode=='1'){
            $scope.mailResponse='Not a valid link. Email not validated. Please register again';
           }
           else if(data.returnCode=='2'){
             $scope.mailResponse='Email validated. Thanks. ';
             alert($scope.mailResponse);
             $location.path('/');
           }
          });
        }
      } ]);
})();