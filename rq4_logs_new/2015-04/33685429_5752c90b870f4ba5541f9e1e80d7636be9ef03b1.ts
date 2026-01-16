// TODO make these into the correct requirements
/// <reference path="../../typings/angularjs/angular.d.ts" />
/// <reference path="../../lib/nAnts/stochastics.d.ts" />


module stochApp {
    'use strict';
    export class SimCtrl {
        /** Configuration
         */
        title = "Title of simulation";
        description = "Description of simulation";
        /* plot settings */
        plotConfig = {
            xDomain: [-2,2],
            yDomain: [-2,2],
        };

        // default data
        params = {
            initial: [0.1, 0],
            /*
            sigma: 1,
            correlation: 0.01,
            tfinal: 40,
            dt: .01,
            /* */
        };
        detParams = [
            {name: 'Speed', default: 1.0},
        ];
        stochParams = [
            {name: 'brownian motion effect', default: 0.1},
        ];
        dims = ['X', 'Y'];

        /** System dynamics (non-stochastic) */
        A(x, t, c) {
            return [ c[0] * x[1], 
                     c[0] * (x[0]-x[0]*x[0]*x[0])];
        }
        /** System dynamics (stochastic) */
        D(x, t, c) {
            return [ c[0], 
                     c[0]];
        }

        static $inject = ['$scope'];
        constructor(private $scope) {
            $scope.mv = this;

            //dummy data
            $scope.plotData = [];
            /* Simulation runner */
            $scope.runSimulation = () => {
                var sol: stochastics.Solution;
                if ($scope.params.method.value === 1)
                    sol = stochastics.euler(this.A, this.D, $scope.params.initial, $scope.params.dt, 
                            $scope.params.tfinal, $scope.params.pA, $scope.params.pD);
                else if ($scope.params.method.value === 2)
                    sol = stochastics.euler(this.A, this.D, $scope.params.initial, $scope.params.dt, 
                            $scope.params.tfinal, $scope.params.pA, $scope.params.pD);
                else
                    sol = stochastics.colour(this.A, this.D, $scope.params.sigma, $scope.params.correlation,
                            $scope.params.initial, $scope.params.dt, $scope.params.tfinal, $scope.params.pA, $scope.params.pD);
                $scope.plotData = sol.getPhase(0,1);
            };
        }
        setUpConfig() {
            /* The textual information */
            this.$scope.title = this.title;
            this.$scope.description = this.description;

            /* Add all the default config */
            this.$scope.plotConfig = this.plotConfig;
            this.$scope.params = this.params;
            this.$scope.detParams = this.detParams;
            this.$scope.stochParams = this.stochParams;
            this.$scope.dims = this.dims;
        }

    }
}