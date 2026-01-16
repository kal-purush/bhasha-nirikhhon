import {app} from './app';

import {TicketController} from './ticket/ticket.controller';

export function initRoutes() {
    app.config(($stateProvider: angular.ui.IStateProvider,
        $urlRouterProvider: angular.ui.IUrlRouterProvider) => {

        $urlRouterProvider.otherwise('/ticket');

        $stateProvider
            .state({

                name: 'ticket',
                url: '/ticket',
                templateUrl: '/src/client/ticket/ticket.html',
                controller: TicketController,
                controllerAs: 'vm'

            });


    });
}