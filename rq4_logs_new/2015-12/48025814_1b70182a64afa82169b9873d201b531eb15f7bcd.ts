///<reference path="../../../typings/tsd.d.ts"/>

import chartSwitcherController = require('./controller');

export = ChartSwitcherDirective;

class ChartSwitcherDirective {
    restrict:string = 'E';
    controller:any = 'htChartSwitcherController';
    controllerAs:string = 'ctrl';
    bindToController:boolean = true;
    templateUrl:string = './directives/chartSwitcher/template.html';
    scope:any = {
        header: '@',
        chartInfo: '='
    };

    static create() {
        return new ChartSwitcherDirective();
    }
}