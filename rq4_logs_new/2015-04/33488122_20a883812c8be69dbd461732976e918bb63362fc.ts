/// <reference path="../scripts/typings/all.d.ts" />

module app {

    export interface IRouteChangeHandler {
        initialize(): void;
    }

    export class RouteChangeHandler implements IRouteChangeHandler {

        $rootScope: ng.IRootScopeService;
        currentUser: app.ICurrentUser;
        eventName: string = '$routeChangeStart';
        loginModalService: app.auth.ILoginModalService;

        static $inject = [
            '$rootScope',
            'currentUser',
            'app.auth.LoginModalService'
        ];
        constructor($rootScope: ng.IRootScopeService,
                    currentUser: app.ICurrentUser,
                    loginModalService: app.auth.ILoginModalService) {
            this.$rootScope = $rootScope;
            this.currentUser = currentUser;
            this.loginModalService = loginModalService;
        }

        initialize() {
            this.$rootScope.$on(this.eventName, (event: ng.IAngularEvent, next: any) => {
                //
                // Authenticated users are always allowed to proceed.
                //
                if (this.currentUser.isAuthenticated) {
                    return;
                }
                //
                // If requesting the root path, then allow the user
                // to proceed.
                //
                if (next && next.originalPath === '/')  {
                    return;
                }
                //
                // Otherwise, do not let the user continue.
                //
                this.loginModalService.show();
                event.preventDefault();
            });
        }

    }

    angular.module('app')
        .service('app.RouteChangeHandler', RouteChangeHandler);

}