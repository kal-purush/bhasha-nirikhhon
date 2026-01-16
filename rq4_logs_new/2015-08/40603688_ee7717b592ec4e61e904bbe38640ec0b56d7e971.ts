/*
 * I need to import the io module to register it with my angular app module.
 * When we import a module and it is not used, the typescript compiler optimized it away.
 */
import IO = require('io/IO'); IO; 

var app = angular.module('app', ['app.io']);

/*
 * Manually bootstrap our angular app
 */
angular.element(document).ready(function () {
    angular.bootstrap(document, ['app']);
});
 