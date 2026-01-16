/// <reference path="../../scripts/typings/all.d.ts" />


var expect = chai.expect;

describe('app.services.CookieService', () => {

    var cookieService: app.services.ICookieService;

    beforeEach(() => {
        angular.mock.module('app.services');
        angular.mock.inject(['app.services.CookieService', (_cookieService) => { cookieService = _cookieService; } ]);
    });

    afterEach(() => {
        cookieService.reset();
    });

    it('can reset the cookies', () => {
        cookieService.username = 'john.doe';
        cookieService.roles = ['admin'];
        cookieService.reset();
        expect(cookieService.username).to.equal(null);
        expect(cookieService.roles).to.equal(null);
    });

    it('can set and get roles', () => {
        cookieService.roles = ['admin', 'developer', 'manager'];
        expect(cookieService.roles[0]).to.equal('admin');
        expect(cookieService.roles[1]).to.equal('developer');
        expect(cookieService.roles[2]).to.equal('manager');
    });

    it('can get and set username', () => {
        cookieService.username = 'john.doe';
        expect(cookieService.username).to.equal('john.doe');
    });

});