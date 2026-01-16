var app = angular.module('demoCA', []);
var jsonVisible = false;

app.controller('formController', function($scope, $http){
	$scope.formData = {};
	$scope.isSubmited = false;
	$scope.isShowing = false;

	
	var processForm = function(){
				
				//=======================================
				//Reset the form display when submiting
				$(".noResult").fadeOut(0);
				$scope.demoDatas = "";
				//=======================================

				$scope.isSubmited = true;
				$scope.isShowing = false;

                $scope.formData.input_format="html";
                $scope.formData.output="json";
                $scope.formData.regions="us";
                $scope.formData.api_key="qUm2Knb7lrav7pgo3GIyjabwIlalLkHufFL/3mMs3Dk=";
	 	console.log($scope.formData);

	 	console.log(showLoading());
	 	 $http({
                        method  : 'POST',                       
                        url: 'https://labs.seleritycorp.com/content-analytics',
                        data: $.param($scope.formData),
                        headers: {'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'}  

	 	 }).success(function(data){
	 	 	
	 	 	$scope.rawDatas = data;
	 	 	$scope.demoDatas = data.results;
	 	 	console.log(data);
	 	 	
	 	 	$scope.isShowing = true;
	 	 	console.log(showLoading());
	 	 	
	 	 	if ($scope.demoDatas[0] == null) {
	 	 		$(".noResult").fadeIn("fast");
	 	 	}else{
	 	 	console.log($scope.demoDatas);};
	 	 })
	 	 .error(
	 	 	console.log("error"),loading = false);

	 }

	 $scope.processForm = processForm;
	 
	 // var showName = function(){
	 // 		jQuery(this).find("span").fadeIn("fast");
	 // }
	 // $scope.showName = showName;
	 var showLoading = function(){
	 	return $scope.isSubmited === true && $scope.isShowing === false ;
	 }
	 $scope.showLoading = showLoading;
});

// app.controller('CAController', function($scope,$http){

	//$http.get("js/demoData.json")
	//.then(function(res){
	// $scope.demoDatas = data.data.results;            
       // });
	//}
	// $scope.processForm = processForm;
// });
app.filter('cut',['$filter', function ($filter) {
        return function (value, wordwise, max, tail) {
            if (!value) return '';

            max = parseInt(max, 10);
            if (!max) return value;
            if (value.length <= max) return value;

            value = value.substr(0, max);
            if (wordwise) {
                var lastspace = value.lastIndexOf(' ');
                if (lastspace != -1) {
                    value = value.substr(0, lastspace);
                }
            }

            return value + (tail || ' …');
        };
    }]);

app.filter('percentage', ['$filter', function ($filter) {
  return function (input, decimals) {
    return $filter('number')(input * 100, decimals) + '%';
  };
}]);



$(window).load(function(){
	var headerHeight = $(".mainNav").height();


	$(".cjson").click(function(){
		if(!jsonVisible){
		$('#jsonContent').fadeIn("fast");
		jsonVisible = true;
		}
		else{
		$('#jsonContent').fadeOut("fast");	
		jsonVisible = false;
		};
	});

	$('.cjson').on('click', function(event) {
    var target = $("#jsonContent");
    var scrollToPosition = $(target).offset().top - headerHeight;
    if( target.length && jsonVisible) {
        // event.preventDefault();
        $('html, body').animate({
            scrollTop: scrollToPosition
        }, 1000);
    }

});



});