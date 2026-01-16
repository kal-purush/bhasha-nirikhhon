var attr=['name', 'type', 'categories', 'abilities', 'weaknesses']

var app = angular.module('myApp', ['firebase']);

app.controller('MainController', ['$scope', '$firebase', function($scope, $firebase){
	var ref = new Firebase('https://sylvesterpoke.firebaseio.com/')
	var sync = $firebase(ref);

	//Firebase Read-only Object
	//$scope.data = sync.$asObject();

	//Firebase 3-way Binding Object
	//var syncObject = sync.$asObject();
	//syncObject.$bindTo($scope, 'data');

	function Pokemon(name, type, cateogries, abilities, weaknesses){
		this.name=name;
		this.type=type;
		this.categories=categories;
		this.abilities=abilities;
		this.weaknesses=weaknesses;
	}

	//Firebase Array
	$scope.pokemonList=sync.$asArray();
	$scope.addPoke=function(){
		$scope.pokemonList.$add($scope.pokemon);
		$scope.pokemon={};
	};

	$scope.deletePoke=function(released){
		$scope.pokemonList.$remove(released);
	};

	$scope.updatePoke=function(updated){
		updated.$save();
	};

}]);