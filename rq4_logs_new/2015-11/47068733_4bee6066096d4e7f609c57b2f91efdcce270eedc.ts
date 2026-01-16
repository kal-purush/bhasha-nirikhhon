/// <reference path="../../../client/typings/app.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

import module from "../../../client/app/services/services.module"
import serv from "../../../client/app/services/auth.service"
var _identityService: mts.IIdentityService,
    _authRequestHandler,
    _localStorageService: any,
    _httpBackend: ng.IHttpBackendService;

describe('Services', () => {
    beforeEach(() => {
        angular.mock.module(module.name, ($provide) => {
            $provide.factory("localStorageService", () => {
                return {
                    get: jasmine.createSpy("get", () => { }),
                    set: jasmine.createSpy("set", () => { }),
                    remove: jasmine.createSpy("remove", () => { })
                };
            });
        });

        angular.mock.inject(($injector: ng.auto.IInjectorService) => {
            _identityService = $injector.get<mts.IIdentityService>('identityService');
            _identityService = $injector.get<mts.IIdentityService>('identityService');
            _httpBackend = $injector.get<ng.IHttpBackendService>('$httpBackend');           
           
        });
    });
    afterEach(function() {
        _httpBackend.verifyNoOutstandingExpectation();
        _httpBackend.verifyNoOutstandingRequest();
    });

    describe('Identity service operations tests', () => {
        it('Identity service should be defined', () => {
            expect(_identityService).toBeDefined();
        });  
    });
});