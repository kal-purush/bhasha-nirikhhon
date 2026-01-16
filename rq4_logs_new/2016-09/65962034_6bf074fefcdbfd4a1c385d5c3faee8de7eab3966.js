angular.module('myApp');
myModule.factory('productFactory', function($http){
	var factory = {};

	factory.addProduct = function(product, callback){
		console.log("IN PRODUCT FACTORY", product)
		$http.post('addProduct', product).success(function(data){
			callback(data)
		})
	}

	factory.getProducts = function(callback){
		$http.get('/getProducts').success(function(data){
			callback(data)
		})
	}

	return factory
})