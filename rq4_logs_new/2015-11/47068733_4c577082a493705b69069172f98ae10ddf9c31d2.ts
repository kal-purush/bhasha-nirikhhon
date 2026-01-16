/// <reference path="../../../client/typings/app.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

import module from "../../../client/app/components/components.module"
import ctrl from "../../../client/app/components/account/signin/signin.controller"


describe("Testing controllers", () => {
    var mockScope,
        controller,
        authService,
        _q: ng.IQService,
        _location: ng.ILocationService,
        signInStub: jasmine.Spy,
        handleMessageStub: jasmine.Spy,
        serverMessageHandler
        ;
    beforeEach(() => {
        angular.mock.module("app")
        angular.mock.module(module.name);
        
        angular.mock.inject(($injector: ng.auto.IInjectorService) => {
            _q = $injector.get<ng.IQService>("$q");
            _location = $injector.get<ng.ILocationService>('$location');
            authService = $injector.get<any>("authService");
            serverMessageHandler = $injector.get<any>("serverMessageHandler");
            signInStub = spyOn(authService, "signIn");
            handleMessageStub = spyOn(serverMessageHandler, "handleMessage");

            mockScope = $injector.get<ng.IRootScopeService>("$rootScope").$new();
            controller = $injector.get<ng.IControllerService>("$controller")(ctrl.controllerId, {
                $scope: mockScope,
                authService: authService
            });
        });
    });
    it("Controller should call server to submit user with uesrname and password", () => {
        signInStub.and.returnValue(_q.when(42))
        controller.user = { username: "John", password: "Doe" };       
        controller.submit();
        expect(authService.signIn).toHaveBeenCalledWith("John", "Doe");
    });
    it("Controller should redirect user to home if request successfull", () => {
        _location.path("/signin");
        signInStub.and.returnValue(_q.resolve("success server res"));        
        controller.user = { username: "John", password: "Doe" };        
        controller.submit();
        mockScope.$digest();
        expect(authService.signIn).toHaveBeenCalledWith("John", "Doe");
        expect(_location.path()).toBe("/");
    });
    it("Controller inform user about error if server request fail", () => {
        _location.path("/signin");
        signInStub.and.returnValue(_q.reject({data:"some_code_error"}));  
        handleMessageStub.and.returnValue("someError"); 
        controller.user = { username: "John", password: "Doe" };        
        controller.submit();
        mockScope.$digest();
        expect(authService.signIn).toHaveBeenCalledWith("John", "Doe");
        expect(_location.path()).toBe("/signin");
        expect(serverMessageHandler.handleMessage).toHaveBeenCalledWith("some_code_error");
        expect(controller.error).toBe("someError");
    });    
}); 