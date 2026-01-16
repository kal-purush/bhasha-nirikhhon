/// <reference path="../../../client/typings/app.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

import module from "../../../client/app/app.module"


let _compile: ng.ICompileService,
    _rootScope: ng.IRootScopeService,
    _identityService: mts.IIdentityService,
    _scope: any,
    _compiledElement: ng.IAugmentedJQuery,
    _isAuthorizedStub: jasmine.Spy,
    _isAuthenticatedStub: jasmine.Spy
    ;
var mcomponentProvider;
describe("Testing Directives", () => {
    describe("Restricted ui directive", () => {
        beforeEach(() => {
            angular.mock.module(module.name);
        });

        beforeEach(() => {
            inject(($compile: ng.ICompileService, $rootScope: ng.IRootScopeService, $injector: ng.auto.IInjectorService) => {
                _compile = $compile;
                _rootScope = $rootScope;
                _identityService = $injector.get<mts.IIdentityService>("identityService");

            });
        })

        function createElement(html, scope): ng.IAugmentedJQuery {
            var element = angular.element(html);
            var compiledElement = _compile(element)(scope.$new());
            scope.$digest();
            return compiledElement;
        }

        beforeEach(() => {
            _scope = _rootScope.$new();
            _scope.model = "";
            _compiledElement = createElement(
                `<div restrict-ui permit-for="user">
                </div>`,
                _scope);
        });

        it("Should be initialized", () => {
            expect(_compiledElement).toBeDefined();
            expect(_compiledElement.scope).toBeDefined;
        });
        it("Should start from hidden state", () => {
            _identityService.user = null;
            _scope.$digest();
            expect(_compiledElement.hasClass('hidden')).toBeTruthy();

        });
        it("Should show element when user authorized for roles", () => {
            _identityService.user = { id: "123456", token: "faketoken", roles: ["user"] };
            _scope.$digest();
            expect(_compiledElement.hasClass('hidden')).toBeFalsy();

        });
        it("Should hide if user has not enouph rights", () => {
            _compiledElement = createElement(
                `<div restrict-ui permit-for="user, admin">
                </div>`,
                _scope);
            _identityService.user = { id: "123456", token: "faketoken", roles: ["user"] };
            _scope.$digest();
            expect(_compiledElement.hasClass('hidden')).toBeTruthy();
        });
        it("Should hide if permit-for is null", () => {
            _compiledElement = createElement(
                `<div restrict-ui>
                </div>`,
                _scope);
            _identityService.user = { id: "123456", token: "faketoken", roles: ["user"] };
            _scope.$digest();
            expect(_compiledElement.hasClass('hidden')).toBeTruthy();
        });
        it("Should hide if roles param is wrong", () => {
            _compiledElement = createElement(
                `<div restrict-ui permit-for="user1">
                </div>`,
                _scope);
            _identityService.user = { id: "123456", token: "faketoken", roles: ["user"] };
            _scope.$digest();
            expect(_compiledElement.hasClass('hidden')).toBeTruthy();
        });
    });
});