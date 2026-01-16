module op.common {
    'use strict';

    /* @ngInject */
    function run(
        $rootScope: ng.IRootScopeService,
        $state: ng.ui.IStateService,
        EVENT_LOGIN_REQUIRED: string) {
        $rootScope.$on(EVENT_LOGIN_REQUIRED, () => {
            $state.go('login');
        });

    }

    angular.module('op.common')
        .run(run);
}