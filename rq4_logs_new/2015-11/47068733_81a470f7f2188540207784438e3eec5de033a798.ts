/// <reference path="../../../client/typings/app.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

import module from "../../../client/app/components/components.module"
import ctrl from "../../../client/app/components/user/psw_SetNew/setNewPassword.controller"


describe("Testing controllers", () => {
    describe("Set new password controller testing", () => {
        var mockScope,
            controller,
            usersService,
            _q: ng.IQService,
            _timeout: any,
            _location: ng.ILocationService,
            setNewStub: jasmine.Spy,
            handleMessageStub: jasmine.Spy,
            serverMessageHandler
            ;
        beforeEach(() => {
            angular.mock.module("app")
            angular.mock.module(module.name);

            angular.mock.inject(($injector: ng.auto.IInjectorService) => {
                _q = $injector.get<ng.IQService>("$q");
                _location = $injector.get<ng.ILocationService>('$location');
                usersService = $injector.get<any>("usersService");
                serverMessageHandler = $injector.get<any>("serverMessageHandler");
                _timeout = $injector.get<any>("$timeout");
                setNewStub = spyOn(usersService, "setNewPassword");
                handleMessageStub = spyOn(serverMessageHandler, "handleMessage");

                mockScope = $injector.get<ng.IRootScopeService>("$rootScope").$new();

                controller = $injector.get<ng.IControllerService>("$controller")(ctrl.controllerId, {
                    $scope: mockScope,  
                    $routeParams: { token: 'faketoken' }
                });
            });
        });
        it("Should get token from route params and call service to set new password if password form is valid", () => {
            setNewStub.and.returnValue(_q.resolve("serverresp"));    
            handleMessageStub.and.returnValue("handledinfo");
            controller.setNewPasswordForm = { $valid: true };
            controller.password = "fakepassword";
            controller.submit();
            mockScope.$digest();
            expect(setNewStub).toHaveBeenCalledWith("fakepassword", "faketoken");
            expect(handleMessageStub).toHaveBeenCalledWith("serverresp");
            expect(controller.info).toBe("handledinfo");
        });
        it("Should set error message if service returns error", () => {
            setNewStub.and.returnValue(_q.reject("serverresp"));    
            handleMessageStub.and.returnValue("handlederror");
            controller.setNewPasswordForm = { $valid: true };
            controller.password = "fakepassword";
            controller.submit();
            mockScope.$digest();
            expect(setNewStub).toHaveBeenCalledWith("fakepassword", "faketoken");
            expect(handleMessageStub).toHaveBeenCalledWith("serverresp");
            expect(controller.error).toBe("handlederror");
        });
        it("Should redirect after 2000 to signin page", () => {
            setNewStub.and.returnValue(_q.resolve("serverresp")); 
            controller.setNewPasswordForm = { $valid: true };
            controller.password = "fakepassword";
            controller.submit();
            mockScope.$digest();
            _timeout.flush();
            _timeout.verifyNoPendingTasks();
            expect(_location.path()).toBe("/signin");
        });

    })
}); 