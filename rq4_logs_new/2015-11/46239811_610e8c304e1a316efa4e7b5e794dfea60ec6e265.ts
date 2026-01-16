/// <reference path="../../.tmp/typings/tsd.d.ts" />


/// <reference path="index.route.ts" />

/// <reference path="index.config.ts" />
/// <reference path="index.run.ts" />
/// <reference path="main/main.controller.ts" />
/// <reference path="../app/components/navbar/navbar.directive.ts" />

module moddynBlog {
  'use strict';

  angular.module('moddynBlog', ['ngAnimate', 'ngCookies', 'ngTouch', 'ngSanitize', 'ui.router', 'ui.bootstrap'])
    .config(Config)
    .config(RouterConfig)
    .run(RunBlock)
    .controller('MainController', MainController)
    .directive('navbar', navbar)
}