// TODO make these into the correct requirements
/// <reference path="../../typings/angularjs/angular.d.ts" />
/// <reference path="../../lib/nAnts/stochastics.d.ts" />


module stochApp {
    'use strict';
    interface MethodParameterList {
        initial: number[];
        sigma: number;
        correlation: number;
        tfinal: number;
        dt: number;
        pA: number[];
        pD: number[];
        method: stochastics.Method;
    }
    interface Parameter {
        name: string;
        default?: number;
    }
    interface IPScope extends ng.IScope {
        vs: MethodParameterList;
        detps: Parameter[];
        stochps: Parameter[];
        methods: any[];
    }
    class ParameterCtrl {
        constructor ($scope: IPScope) {

            // default data
            $scope.vs.initial = [0.1, 0.1];
            $scope.vs.sigma = 1;
            $scope.vs.correlation = 0.01;
            $scope.vs.tfinal = 40;
            $scope.vs.dt = .01;
            $scope.vs.pA = [];
            for (var i in detps)
                $scope.vs.pA.push(detps.default || 0);
            $scope.vs.pD = [];
            for (var i in stochps)
                $scope.vs.pA.push(stochps.default || 0);

            // Methods
            $scope.methods = [
                {label: 'Euler-Maruyama', value: stochastics.Euler},
                {label: 'Milstein', value: stochastics.Mistein},
                {label: 'Coloured', value: stochastics.Colour},
            ];
            $scope.p.method = $scope.methods[0];
        }
    }

    export function parameterDirective () {
        return {
            restrict: 'E',
            scope: {
                vs: '=values',
                detps: '=det-params',
                stochps: '=stoch-params',
            },
            controller: ParameterCtrl,
            template:
`<fieldset>
    <legend>Non-Stochastic Parameters</legend>
    <p ng-repeat="p in detps">
        <input id="pA-{{$index}}" type="number" ng-model="vs.pA[$index]" />
        <label for="pA-{{$index}}">{{ p.name }}</label>
    </p>
</fieldset>
<fieldset>
    <legend>Stochastic Parameters</legend>
    <p ng-repeat="p in stochps">
        <input id="pD-{{$index}}" type="number" ng-model="vs.pD[$index]" />
        <label for="pD-{{$index}}">{{ p.name }}</label>
    </p>
</fieldset>
<fieldset>
    <legend>Numerical Scheme</legend>
    <p>
        <select id="sim-select" ng-model="method"
            ng-options="m as m.label for m in methods">
        </select>
        <label for="sim-select">Simulation Method</label>
    </p>
    <p>
        <input id="time-correlation" type="number" ng-model="correlation" />
        <label for="time-correlation">Time correlation</label>
    </p>
    <p>
        <input id="dt" type="number" ng-model="dt" />
        <label for="dt">Time-step</label>
    </p>
    <p>
        <input id="t-final" type="number" ng-model="tfinal" />
        <label for="t-final">Final Time</label>
    </p>
    <p>
        <input id="x0" type="number" ng-model="initial[0]" />
        <label for="x0">Initial X position</label>
    </p>
    <p>
        <input id="y0" type="number" ng-model="initial[1]" />
        <label for="y0">Initial Y position</label>
    </p>
</fieldset>`,
        }
    }
}