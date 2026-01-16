/// <reference path="../../scripts/typings/all.d.ts" />

var expect = chai.expect;

describe('NavigationController', () => {

    var ctrl: app.layout.INavigationScope;
    var currentUser: ICurrentUser;

    beforeEach(() => {
        currentUser = new app.CurrentUser();
        ctrl = new app.layout.NavigationController(currentUser);
        angular.mock.module('app.layout');
        angular.mock.inject(($controller) => {
            ctrl = $controller('app.layout.NavigationController', {
                'currentUser': currentUser
            });
        })
    });

    describe('isAuthenticated', () => {

        it('returns false when the username is null', () => {
            currentUser.username = null;
            expect(ctrl.isAuthenticated).to.equal(false);
        });

        it('returns true when username is not null', () => {
            currentUser.username = 'john.doe';
            expect(ctrl.isAuthenticated).to.equal(true);
        });

    });

    describe('username', () => {

        it('returns the current user username', () => {
            currentUser.username = 'my_test_username';
            expect(ctrl.username).to.equal('my_test_username');
        });

    });

});