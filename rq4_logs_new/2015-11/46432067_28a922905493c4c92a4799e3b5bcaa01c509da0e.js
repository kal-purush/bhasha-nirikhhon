angular.module('ecommerce', ['ui-router']).config(function ($stateProvider, $urlRouterProvider, $httpProvider) {
	$urlRouterProvider.otherwise('/');

	// Routing Configuration Code //
	$stateProvider
	// home state //
		.state('home', {
			url: '/',
			templateUrl: 'home/homeTmpl.html',
			controller: 'home/homeCtrl.js',
			resolve: {
				productList: function ($http) {
					return $http.get('/api/products').then(function (productListResponse) {
						return productListResponse;
					});
				}
			}
		// main state //
				.state('main', {
					url: '/main',
					templateUrl: 'js/main/mainTmpl.html',
					controller: 'mainCtrl',
					resolve: {
						productList: function ($http) {
							return $http.get('/api/products').then(function (productListResponse) {
								return productListResponse.data.data;
							});
						}
					}
				})
		});

});