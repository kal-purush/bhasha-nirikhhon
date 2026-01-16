angular.module('app')

.controller('checkboxController', function(checkboxService, $scope) {
	$scope.toggleSelection = function(){
		checkboxService.setSelectedItems(event);
	}
})

.controller('criteriaController', function(criteriaService, $scope) {
	angular.element(document).ready(function () {
		$scope.criteria = 'views';
		criteriaService.setCriteria($scope.criteria);
    });
	
	$scope.chooseCriteria = function(selectedCriteria) {
		criteriaService.setCriteria(selectedCriteria);
	}
})

.controller('maxResultsController', function(maxResultsService, $scope){
	angular.element(document).ready(function () {
		$scope.maxResults = '5';
		maxResultsService.setMaxResults($scope.maxResults);
    });
	
	$scope.getMaxResults = function(maxResults) {
		maxResultsService.setMaxResults(maxResults);
	}
})

.controller('keywordController', function(keywordService, $scope){
	$scope.setKeyword = function(){
		keywordService.setKeyword(event);
	}
})

.controller('searchController', function(checkboxService, criteriaService, maxResultsService, keywordService, searchService, $scope, $rootScope) {
	$scope.startSearch = function(){
		$scope.selectedCategories = checkboxService.getSelectedItems();
		$scope.selectedCriteria = criteriaService.getCriteria();
		$scope.maxResults = maxResultsService.getMaxResults();
		$scope.keyword = keywordService.getKeyword();
		
		$scope.validationStatus = 
			searchService.validateInputs($scope.selectedCriteria, $scope.selectedCategories, $scope.keyword, $scope.maxResults);
		
		console.log("Validation status: ", $scope.validationStatus);
		
		if($scope.validationStatus === "VALID") {
			$rootScope.$broadcast('clearResults');
			var spinner = angular.element(document.getElementById('progressBar'));
			spinner.show();
			
			if($scope.selectedCriteria == "views") {
				var promise = searchService.doSearchViews($scope.selectedCriteria, $scope.selectedCategories, $scope.keyword, $scope.maxResults);
				promise.then(function(response){
					$rootScope.$broadcast('searchResultsObtained', response.data);
				});
			}
			else if($scope.selectedCriteria == "popular") {
				var promise = searchService.doSearchPopular($scope.selectedCriteria, $scope.selectedCategories, $scope.maxResults);
				promise.then(function(response){
					$rootScope.$broadcast('searchResultsObtained', response.data);
				});
			}
		}
	}
})

.controller('displayResultsController', function($scope, $rootScope){
	$rootScope.$on('clearResults', function() {
		$scope.searchResults = {};
	});
	
	$rootScope.$on('searchResultsObtained', function(event, response){
		var spinner = angular.element(document.getElementById('progressBar'));
		spinner.hide();
		$scope.searchResults = response;
	});
})

.controller('loginController', function(routeService, $http, $scope, $rootScope, $window, $location){
	console.log("Initializing loginController");
	
	var clientId = '386721466748-hc6or76387qevpajh4iupstfsn11dc83.apps.googleusercontent.com';
	var apiKey = 'AIzaSyDXz-f_jsDJZ8mF46OUjcNPjplnDtyeqeA';
	var scopes = ['https://www.googleapis.com/auth/yt-analytics.readonly', 'https://www.googleapis.com/auth/youtube.readonly'];

	angular.element($window).bind('load', function() {
		handlePageLoad();
	});
	
	function handlePageLoad() {
		gapi.client.setApiKey(apiKey);
		window.setTimeout(checkAuth,1);
		console.log("Client loaded...");
	}
			
    function checkAuth() {
      	gapi.auth.authorize({client_id: clientId, scope: scopes, immediate: true}, handleAuthResult);
    }
    
    function handleAuthResult(authResult) {
      	var authorizeButton = document.getElementById('authorize-button');
      	console.log(authResult);
      	if (authResult && !authResult.error) {
      		console.log("Auth successful!");
        	authorizeButton.style.visibility = 'hidden';
      	} else {
      		console.log("Auth result error...");
        	authorizeButton.style.visibility = '';
        	authorizeButton.onclick = handleAuthClick;
      	}
    }
    
    function handleAuthClick(event) {
    	console.log("Handling Auth Click");
      	gapi.auth.authorize({client_id: clientId, scope: scopes, immediate: false}, handleAuthResult)
      		.then(function(){        	
      			$window.location = './#/dashboard';
      			console.log("redirect should've occurred")
      		});
      	return false;
    }
    
    
    function loadAPIClientInterfaces() {
        gapi.client.load('youtube', 'v3', function() {
          gapi.client.load('youtubeAnalytics', 'v1', function() {
            // After both client interfaces load, use the Data API to request
            // information about the authenticated user's channel.
            getUserChannel();
          });
        });
      }

    function getUserChannel() {
    	
        var request = gapi.client.youtube.channels.list({
          // Setting the "mine" request parameter's value to "true" indicates that
          // you want to retrieve the currently authenticated user's channel.
          mine: true,
          part: 'id,contentDetails'
        });

        request.execute(function(response) {
          if ('error' in response) {
            console.log(response.error.message);
          } else {
            // We need the channel's channel ID to make calls to the Analytics API.
            // The channel ID value has the form "UCdLFeWKpkLhkguiMZUp8lWA".
            channelId = response.items[0].id;
            // Retrieve the playlist ID that uniquely identifies the playlist of
            // videos uploaded to the authenticated user's channel. This value has
            // the form "UUdLFeWKpkLhkguiMZUp8lWA".
            var uploadsListId = response.items[0].contentDetails.relatedPlaylists.uploads;
            // Use the playlist ID to retrieve the list of uploaded videos.
            getPlaylistItems(uploadsListId);
          }
        });
      }
    
});