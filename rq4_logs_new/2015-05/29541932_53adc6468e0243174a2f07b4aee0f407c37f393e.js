'use strict';

function directive() {
    return {
        restrict: 'E',

        link: function (elem, attr) {

        }
    };
}


angular
    .module('electoralApp')
    .directive('distrito', distrito);

directive.$inject = [];