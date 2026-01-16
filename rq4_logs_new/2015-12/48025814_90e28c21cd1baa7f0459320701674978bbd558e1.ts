///<reference path="../../../typings/tsd.d.ts"/>

import statisticsController = require('./controller');

export = StatisticsDirective;

class StatisticsDirective {
    restrict:string = 'E';
    controller:any = 'htStatisticsController';
    controllerAs:string = 'ctrl';
    bindToController:boolean = true;
    templateUrl:string = './directives/statistics/template.html';
    scope:any = {};

    static create() {
        return new StatisticsDirective();
    }
}