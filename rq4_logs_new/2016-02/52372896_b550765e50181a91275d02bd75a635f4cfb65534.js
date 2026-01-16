angular.module('githubDashboard.user-detail', [
  'ui.router'
])
  .config(function($stateProvider) {
    $stateProvider
      .state('userDetail', {
        url: '/users/:username',
        templateUrl: 'users/user-detail/user-detail.tpl.html',
        controller: 'UserDetailCtrl as userDetail'
      })
  })
  .controller('UserDetailCtrl', function($stateParams, userService) {
    var userDetail = this;

    userService.getUser($stateParams.username).then(function(user) {
      userDetail.user = user;
    })
  })
;