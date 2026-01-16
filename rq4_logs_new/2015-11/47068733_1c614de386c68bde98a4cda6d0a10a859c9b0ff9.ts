/// <reference path="../../../client/typings/app.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

import module from "../../../client/app/services/services.module"

var _config,
    _messageService
    ;

describe('Testing services', () => {
    beforeEach(() => {
        angular.mock.module(module.name, ($provide) => {
            $provide.value("config", {
                serverData: {
                    fake_key: {
                        default: "default"
                    }
                }
            });
        });

        angular.mock.inject(($injector: ng.auto.IInjectorService) => {
            _config = $injector.get<mts.IIdentityService>('config');
            _messageService = $injector.get<mts.IIdentityService>('serverMessageHandler');

        });
    });

    describe('Message service operations tests', () => {
        it('Message service should be defined, server configuration imported', () => {
            expect(_messageService).toBeDefined();
            expect(_messageService.serverData).toEqual(_config.serverData);
        });
        it('Message service should create report from error code as message', () => {
            var result = _messageService.handleMessage({key:"fake_key", message: "fakeMessage"})
            expect(result).toEqual("fakeMessage");
        });
        it('Message service should create report from error code as default', () => {
            var result = _messageService.handleMessage({key:"fake_key"})
            expect(result).toEqual("default");
        });
        it('Should fail to find error message and return unexpected server error', () => {
            var result = _messageService.handleMessage({key:"fake_key_not_configured"})
            expect(result).toEqual("Unexpected server error");
        });
    });
});