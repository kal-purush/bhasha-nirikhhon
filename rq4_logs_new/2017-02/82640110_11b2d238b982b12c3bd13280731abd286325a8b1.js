angular.module( "lotour.app").directive("footerDirective",function(){

	return {
		templateUrl : "components/footer/footer.html",
		restrict : "E",
		replace : true,
		controller : function( $scope ){
			$scope.footerHide = false;
			$scope.$on("hasHide",function(event,data){
				$scope.footerHide = data;
			})
		}
	}


})