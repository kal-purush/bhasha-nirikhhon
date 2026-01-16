module op.common {
    'use strict';

    beforeEach((): void => {
        module('op.common');
    });

    describe('op.common.ToolbarController', (): void => {
        var createController: Function;
        var scope: any;

        function injected($injector: ng.auto.IInjectorService): void {
            var $rootScope: ng.IRootScopeService = $injector.get('$rootScope');
            var $controller: ng.IControllerService = $injector.get('$controller');
            scope = $rootScope.$new();
            createController = function(): void {
                $controller('ToolbarController as toolbar', {
                    $scope: scope
                });
            }
        }

        beforeEach(inject(injected));

        it('should be defined', (): void => {
            createController();
            expect(scope.toolbar).toBeDefined();
        });
    });

}