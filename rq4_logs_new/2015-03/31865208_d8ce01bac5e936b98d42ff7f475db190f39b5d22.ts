module op.common {
    'use strict';

    beforeEach((): void => {
        module('op.common');
    });

    describe('op.common.MenuController',(): void => {
        var createController: Function;
        var scope: any;

        function injected($injector: ng.auto.IInjectorService): void {
            var $rootScope: ng.IRootScopeService = $injector.get('$rootScope');
            var $controller: ng.IControllerService = $injector.get('$controller');
            scope = $rootScope.$new();
            createController = function(): void {
                $controller('MenuController as menu', {
                    $scope: scope
                });
            }
        }

        beforeEach(inject(injected));

        it('should be defined',(): void => {
            createController();
            expect(scope.menu).toBeDefined();
        });
    });

}