var app = angular.module('App', []);
app.controller('AppController', function($scope) {
	$scope.contacts = [
		{
			name: 'Mark',
			age: 34,
			gender: 'Male'
		},
		{
			name: 'Sarah',
			age: 29,
			gender: 'Female'
		},
		{
			name: 'Jacob',
			age: 5,
			gender: 'Male'
		},
		{
			name: 'Abigail',
			age: 0,
			gender: 'Female'
		}];	
});