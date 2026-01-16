///<reference path='../../../typings/tsd.d.ts' />
module GameField {
  'use strict';

  /**
  * @ngdoc directive
  * @name game.directive:gameField
  * @restrict EA
  * @element
  *
  * @description
  *
  * @example
    <example module="game">
      <file name="index.html">
        <game-field></game-field>
      </file>
    </example>
  *
  */
  angular
    .module('game')
    .directive('gameField', gameField);

  function gameField(): ng.IDirective {
    return {
      restrict: 'EA',
      scope: {},
      templateUrl: 'game/directives/game-field-directive.tpl.html',
      replace: false,
      controllerAs: 'gameField',
      controller: function () {
        var vm = this;
        vm.name = 'gameField';
      },
      link: function (scope: ng.IScope, element: JQuery, attrs: any) {
        /*jshint unused:false */
        /*eslint "no-unused-vars": [2, {"args": "none"}]*/
      }
    };
  }
}