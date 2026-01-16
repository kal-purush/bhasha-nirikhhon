/// <reference path="components/base-controller.ts" />

module stochApp {
    'use strict';
    /* A Sort of double well system, but really it's streamlines,
     * because the stochastic term affects all dimensions
     */
    export class MyCtrl extends SimCtrl {
        /*** The rest can be changed to suit the problem at hand ***/

        title = "Double well Hamiltonian example";
        description = `This is a simple example of the stochastic simulation framework. The dynamical system is defined as the Hamiltonian of a potential well \\ \[ \\phi(x,y) = 1/4 x^4 - 1/2 x^2 + 1/2 y^2 \\ \]
        with brownian motion. All the simulation parameters are adjustible including the strength of the dynamics vs. the stochastic terms.`

        /* plot settings */
        plotConfig = {
            xDomain: [-2,2],
            yDomain: [-2,2],
        };

        // default parameters
        params = {
            initial: [0.5, 0],
            /*
            sigma: 1,
            correlation: 0.01,
            tfinal: 40,
            dt: .01,
            /* */
        };
        // System parameters
        detParams = [
            {name: 'Speed', default: 1.0},
        ];
        stochParams = [
            {name: 'brownian motion effect', default: 0.1},
        ];
        dims = ['X', 'Y'];

        /** System dynamics (non-stochastic) 
         * In this case, it is the hamiltonian of \phi(x, y) = 1/4 x^4 - 1/2 x^2 + 1/2 y^2
         */
        A(x, t, c) {
            return [ c[0] * x[1], 
                     c[0] * (x[0]-x[0]*x[0]*x[0])];
        }
        /** System dynamics (stochastic) 
         * Brownian motion
         */
        D(x, t, c) {
            return [ c[0], 
                     c[0]];
        }
        constructor($scope) { super($scope); this.setUpConfig();
            /* Custom functions and behaviour can go here */
        }
    }
}