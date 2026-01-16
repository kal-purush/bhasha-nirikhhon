/// <reference path="../../../client/typings/app.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

import module from "../../../client/app/services/services.module"
import serv from "../../../client/app/services/auth.service"
var authService: any,
    identityService: mts.IIdentityService,
    authRequestHandler,
    httpBackend: ng.IHttpBackendService;

describe('pageMetaService', () => {
    beforeEach(() => {
        angular.mock.module(module.name, ($provide) => {
            $provide.factory("identityService", () => {
                return {
                    user: {}
                };
            });
        });

        angular.mock.inject(($injector: ng.auto.IInjectorService) => {
            identityService = $injector.get<mts.IIdentityService>('identityService');
            httpBackend = $injector.get<ng.IHttpBackendService>('$httpBackend');
            authRequestHandler = httpBackend.when('GET', '/auth.py')
                .respond({ userId: 'userX' }, { 'A-Token': 'xxx' });
            authService = $injector.instantiate(serv);
        });
    });
    afterEach(function() {
        httpBackend.verifyNoOutstandingExpectation();
        httpBackend.verifyNoOutstandingRequest();
    });

    describe('Service operation tests', () => {
        it('Identity service should be defined', () => {
            expect(authService.identityService).toBeDefined();
        });
        it("Should sign up and return user with token test when http 200", () => {
            httpBackend
                .expectPOST("/auth/signup")
                .respond(200, {
                    _id: "12345",
                    token: "faketoken"
                });
            spyOn(authService, 'signUp').and.callThrough();
            authService.signUp().then(() => {
                expect(authService.signUp).toHaveBeenCalled();
                expect(identityService.user._id).toBe("12345");
                expect(identityService.user.token).toBe("faketoken");
            });
            httpBackend.flush();
        });
        it("Should sign in and return user with token test when http 200", () => {
            httpBackend
                .expectPOST("/auth/signin", { username: "name", password: "pass" })
                .respond(200, {
                    _id: "12345",
                    token: "faketoken"
                });
            spyOn(authService, 'signIn').and.callThrough();
            authService.signIn("name", "pass").then(() => {
                expect(authService.signIn).toHaveBeenCalled();
                expect(identityService.user._id).toBe("12345");
                expect(identityService.user.token).toBe("faketoken");
            });
            httpBackend.flush();
        });
        it("Should sign out and and reset current user", () => {
            httpBackend
                .expectPOST("/auth/signout")
                .respond(200, "OK");
            spyOn(authService, 'signOut').and.callThrough();
            authService.signOut().then(() => {
                expect(authService.signOut).toHaveBeenCalled();
                expect(identityService.user).toBeNull();
            });
            httpBackend.flush();
        });
    });
});