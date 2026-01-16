/**
 * Created by dhnishi on 4/12/15.
 */

/// <reference path="../../../typings/angularjs/angular.d.ts"/>

/// <reference path="../services/BattleEntitiesService.ts"/>
/// <reference path="../Entity.ts"/>
/// <reference path="../main.ts"/>

app.controller("BattleController", ["battleEntitiesService", BattleController]
);

function BattleController(battleEntitiesService) {
    var battleCtrl = this;
    battleCtrl.helloWorld = "Hello World";
    battleCtrl.entities = battleEntitiesService.entities;

    battleEntitiesService.addEntity(new Entity());
}