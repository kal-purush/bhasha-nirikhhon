//Angular js app
var app = angular.module('readersHub', ['angularUtils.directives.dirPagination']);

//app controller declaration
app.controller('readersHubController', ["$scope", "$http", "FeedService", readersHubController]);

//faactory that parses feeds to json
app.factory('FeedService',['$http',function($http){
    return {
        parseFeed : function(url){
            return $http.jsonp('//ajax.googleapis.com/ajax/services/feed/load?v=1.0&num=50&callback=JSON_CALLBACK&q=' + encodeURIComponent(url));
        }
    }
}]);