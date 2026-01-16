var app = angular.module('myApp',['ngResource']).factory('Local', function ($resource) {
    /*var data = $resource('/api/lugares', {
      locais:{
        method:'GET'
      }
    });
    return local;*/
    return $resource('http://getaplace.herokuapp.com/api/lugares/');
});

//
// angular.module('myApp.controllers',[]);
//
// angular.module('myApp.controllers').controller('ResourceController',function($scope, LocalService) {
//   $stateProvider.state('locais', { // state for showing all movies
//     url: '',
//     controller: 'LocaisListController'
//   });
// }).run(function($state) {
//   $state.go('locais'); //make a transition to movies state when app starts
// });