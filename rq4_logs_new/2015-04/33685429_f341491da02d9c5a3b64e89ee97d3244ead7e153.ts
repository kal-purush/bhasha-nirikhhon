// TODO make these into the correct requirements
/// <reference path="../typings/angularjs/angular.d.ts" />
/// <reference path="../lib/nAnts/stochastics.d.ts" />


module stochApp {
    'use strict';
    export function SimCtrl($scope) {
        var dorbit = function (x,t,c) {
            return [ c[0] * x[1], 
                     c[0] * (x[0]-x[0]*x[0]*x[0])];
        }
        
        var dwell = function (x,t,c) {
            return [ c[0] * (x[0] - x[0]*x[0]*x[0]),
                     - c[0] * (x[1])];
        }

        var dconstant = function (x,t,c) {
            return [ c[0], 
                     c[0]];
        }

        /* plot settings */
        $scope.plotConfig = {
            xDomain: [-2,2],
            yDomain: [-2,2],
        }
        //dummy data
        $scope.plotData = [ [-4,-2], [-2,1], [0,-1], [2,2], [4,-2]];

        // default data
        $scope.initial = [0.1, 0.1];
        $scope.sigma = 1;
        $scope.correlation = 0.01;
        $scope.tfinal = 40;
        $scope.dt = .01;
        $scope.pA = [1.0];
        $scope.pD = [0.1];

        // Methods
        $scope.methods = [
            {label: 'Euler-Maruyama', value: 'euler'},
            {label: 'Milstein', value: 'milstein'},
            {label: 'Coloured', value: 'colour'},
        ];
        $scope.method = $scope.methods[0];

        /* Simulation runner */
        $scope.runSimulation = function () {
            var sol: stochastics.Solution;
            if ($scope.method.value === 'euler')
                sol = stochastics.euler(dorbit, dconstant, $scope.initial, $scope.dt, 
                        $scope.tfinal, $scope.pA, $scope.pD);
            else if ($scope.method.value === 'milstein')
                sol = stochastics.euler(dorbit, dconstant, $scope.initial, $scope.dt, 
                        $scope.tfinal, $scope.pA, $scope.pD);
            else
                sol = stochastics.colour(dorbit, dconstant, $scope.sigma, $scope.correlation,
                        $scope.initial, $scope.dt, $scope.tfinal, $scope.pA, $scope.pD);
            $scope.plotData = sol.getPhase(0,1);
        };
    }
}