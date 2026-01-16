/// <reference path="../../scripts/typings/all.d.ts" />

var expect = chai.expect;
describe('app.auth.LogoutService', (): void => {

    var currentUser: any;
    var localStorage: any;
    var logoutService: app.auth.ILogoutService;

    beforeEach(() => {
        angular.mock.module(
            'app.auth',
            ($provide) => {
                $provide.service('currentUser', () => {
                    currentUser = {
                        username: 'john.doe',
                        roles: ['admin', 'developer']
                    };
                    return currentUser;
                });
                $provide.service('localStorage', () => {
                    localStorage = {
                        removeItem: () => {}
                    };
                    return localStorage;
                });
            }
        );
        angular.mock.inject([
            'app.auth.LogoutService',
            (_logoutService) => {
                logoutService = _logoutService;
            }
        ]);
    });

    describe('logout()', () => {

        it('calls removeItem to remove roles', () => {
            var spy = sinon.spy(localStorage, 'removeItem');
            logoutService.logout();
            expect(spy.calledWith('roles')).to.equal(true);
        });

        it('calls removeItem to remove username', () => {
            var spy = sinon.spy(localStorage, 'removeItem');
            logoutService.logout();
            expect(spy.calledWith('username')).to.equal(true);
        });

        it('sets the roles to an empty array', () => {
            logoutService.logout();
            expect(currentUser.roles.length).to.equal(0);
        });

        it('sets the username to null', () => {
            logoutService.logout();
            expect(currentUser.username).to.equal(null);
        });

    });

});