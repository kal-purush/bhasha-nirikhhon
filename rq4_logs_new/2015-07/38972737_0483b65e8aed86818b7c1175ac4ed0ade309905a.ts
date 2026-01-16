/// <reference path="../../typings/tsd" />

import angular = require('angular');
import ngRoute = require('angular-route');

let app = angular.module('myApp', [ngRoute]);

app.config(($routeProvider: ng.route.IRouteProvider) => {
		$routeProvider.when('/', {
			template: ['<p>{{vm.say("hogehoge")}}</p>'].join(),
			controller: MainCtrl,
			controllerAs: 'vm'
		});
	});

// [MEMO] 依存解決のためにこのファイル内で使用しないけどimportする。
//        "angular.module('myApp', [])"の後に書くこと。
import MainCtrl = require('./main/main.controller');