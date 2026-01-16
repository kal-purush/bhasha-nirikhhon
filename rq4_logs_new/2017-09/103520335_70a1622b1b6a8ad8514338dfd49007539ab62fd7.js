
var data = '';
var a = angular.module("myApp", ['ngSanitize', 'ngRoute']);

a.controller('mainController', function($scope, $http, $window){
	
	$http({
      	method: "GET",
      	url: "data/data.json"
      }).then(function mySuccess(response){

      		$scope.title = response.data.config.title;
      		
      		// twitter card
      		$scope.twitter_card 	= response.data.config.meta.twitter.card;
      		$scope.twitter_site 	= response.data.config.meta.twitter.site;
      		$scope.twitter_creator 	= response.data.config.meta.twitter.creator;

      		// facebook open graph
      		$scope.fb_type 	= response.data.config.meta.facebook.type;
      		$scope.fb_url 	= response.data.config.meta.facebook.url;
      		$scope.fb_title 	= response.data.config.meta.facebook.title;
      		$scope.fb_desc 	= response.data.config.meta.facebook.description;
      		$scope.fb_image 	= response.data.config.meta.facebook.image;

                  $scope.header = { 
                                    title: response.data.header.title,
                                    subtitle: response.data.header.subtitle
                                    };

                  $scope.parallax = response.data.header.parallax;
                  $scope.bg_header = response.data.header.background;
      		
      });

      $window.onload = function(){

                        $(window).scroll(function (event) {
                            var scroll = $(window).scrollTop();
                            console.log(scroll);
                            if( scroll != 0){
                              $('.navbar-default').addClass('sticky');
                            }else{
                              $('.navbar-default').removeClass('sticky');
                            }
                        });
                  }
})
.directive('headerElement', function(){
      return {
            restrict: "A",
            scope: {
                  headerInfo : '=info'
            },
            template: '<h3> {{ headerInfo.title }}</h3><hr /><h4>{{ headerInfo.subtitle}}</h4>'
            
      }
});
