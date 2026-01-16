/// <reference path="../../typings/main.d.ts"/>
/// <reference path="./ScriptServer.ts"/>

const ScriptServer = require("./ScriptServer");
const angular = require("angular");

export class Script {
  _name: string;
  _command: string;
  _interval: number;
  constructor(name, command, interval) {
    this._name = name;
    this._command = command;
    this._interval = interval;
  }
  exec() {
    let app = angular.module(this._name, []);
    let script = new ScriptServer(this._name, this._command, this._interval);
    app.controller("body", ["$scope", "$interval", ($scope, $interval) => {
      $interval( () => {
        let result = script.exec();
        $scope.$apply(function () { // $apply is needed to be displayed ASAP (because of AJAX callback)
          $scope.stdout = result;
      }), this._interval});
    }]);
  }


//
}