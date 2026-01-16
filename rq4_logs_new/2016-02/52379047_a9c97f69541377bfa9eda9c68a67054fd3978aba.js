var correo = angular.module("correo",[]);

var resultados = [{"equipo1" : "Atletico Madrid",
					"equipo2" : "Villarreal",
					"resultado" : "0-0"},
					{"equipo1" : "Real Madrid",
					"equipo2" : "Malaga",
					"resultado" : "1-1"}]

correo.controller("MainCtrl", function($scope)
{
	$scope.resultados = resultados;

});