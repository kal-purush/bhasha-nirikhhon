angular.module('hairy-boo', ['ngMaterial', 'ui.router'])
  .config(function($stateProvider, $urlRouterProvider) {
    $urlRouterProvider.otherwise('/mapa');
  
    $stateProvider
      .state('mapa', {
        url: '/mapa',
        templateUrl: 'src/mapa/view/mapa.html'
      })
      .state('cacambas', {
        url: '/cacambas',
        templateUrl: 'src/cacambas/view/cacambas.html'
      });
  });