/// <reference path="../../../client/typings/app.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

import module from "../../../client/app/directives/directives.module"


let _compile: ng.ICompileService,
    _rootScope: ng.IRootScopeService,
    _scope: any,
    _compiledElement: ng.IAugmentedJQuery;
var mcomponentProvider;
describe("Testing Directives", () => {

    describe("Password Checker", () => {

      

        beforeEach(angular.mock.module(module.name, ($provide: ng.auto.IProvideService) => {
            $provide.factory("logger", () => {
                var warning = jasmine.createSpy("warning");
                var error = jasmine.createSpy("error");
                var info = jasmine.createSpy("info");
                return {
                    warning: warning,
                    error: error,
                    info: info
                }
            });
        }));

        beforeEach(inject(($compile: ng.ICompileService, $rootScope: ng.IRootScopeService) => {
            _compile = $compile;
            _rootScope = $rootScope;
        }
        ));

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
                "<div><input  type='password'  ng-model='model' required password-strength-checker strength></div>", _scope);
        });

        it("Should be initialized", () => {
            expect(_compiledElement).toBeDefined();
            expect(_compiledElement.scope).toBeDefined;
        });
        it("Should apply elements css", () => {
            var input = _compiledElement.find("input");
            expect(input.hasClass("psw")).toBeTruthy();
        });
        it("Should append span with image", () => {
            let input = _compiledElement.find("input");
            let addedSpan = _compiledElement.find("span");
            expect(_compiledElement.find("span")).toBeDefined();
        });
        it("Should set default style class to input", () => {
            let input = _compiledElement.find("input");
            expect(input.hasClass("psw-default")).toBeTruthy();
        });
        it("Should bind value from model", () => {
            _scope.model = "1";
            _scope.$digest();
            let input = _compiledElement.find("input");
            expect(input.hasClass("psw-default")).toBeTruthy();
            expect(input.val()).toBe("1");
        });
        it("Should change style class to danger when value changes", () => {
            _scope.model = "1";
            _scope.$digest();
            let input = _compiledElement.find("input");
            expect(input.hasClass("psw-danger")).toBeTruthy();
        });
        it("Should change style class to good when value changes", () => {
            _scope.model = "123456";
            _scope.$digest();
            let input = _compiledElement.find("input");
            expect(input.hasClass("psw-good")).toBeTruthy();
            expect(input.hasClass("psw-danger")).toBeFalsy();
        });
        it("Should change style class to strong when value changes", () => {
            _scope.model = "123456Ad-";
            _scope.$digest();
            let input = _compiledElement.find("input");
            expect(input.hasClass("psw-good")).toBeFalsy();
            expect(input.hasClass("psw-danger")).toBeFalsy();
            expect(input.hasClass("psw-strong")).toBeTruthy();
        });
    });
});