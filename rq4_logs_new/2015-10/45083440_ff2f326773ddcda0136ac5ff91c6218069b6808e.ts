/// <reference path='../libs/jquery/jquery.d.ts' />
/// <reference path='../libs/angular/angular.d.ts' />
/// <reference path='../libs/angular/angular-route.d.ts' />

/// <reference path='viewblogposts/ViewBlogPostCtrl.ts' />

module blogposts {
    'use strict';

    var golby = angular.module('golby', ['ngRoute'])
            .controller('viewBlogPostCtrl', ViewBlogPostCtrl)
            .config(['$routeProvider',
                function routes($routeProvider: ng.route.IRouteProvider) {
                    $routeProvider
                        .when('/', {
                            templateUrl: 'views/view1.html',
                            controller: 'viewBlogPostCtrl'
                        })
                        .when('/cheese', {
                            templateUrl: 'views/view2.html',
                            controller: 'viewBlogPostCtrl'
                        })
                        .otherwise({
                            redirectTo: '/'
                        });
                }
            ]);
}