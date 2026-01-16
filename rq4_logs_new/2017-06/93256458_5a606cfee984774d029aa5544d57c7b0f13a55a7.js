app.directive('clueList', function() {
	return {
		restrict : 'E',
		scope : {
			cluelist : '='
		},
		link : function(scope, elm, attr) {
			console.log(scope.cluelist);
		},
		templateUrl : '/javascripts/directives/clueList.html'
	}
});