/**
 * Created by dmorales on 21/03/2016.
 */

var app = angular.module("twitterApp", []);

app.controller("AppCtrl", function() {
    var appCtrl = this;

    appCtrl.loadMoreTweets = function() {
        alert("Loading Tweets");
    };

    appCtrl.deleteTweets = function() {
        alert("deleting Tweets");
    };
});

app.directive("enter", function() {
    return function(scope, element, attrs) {
        element.bind("mouseenter", function() {
            scope.$apply(attrs.enter)
        });
    };
});
