/// <reference path="../../scripts/typings/all.d.ts" />

var expect = chai.expect;
describe('localStorage', (): void => {

    var localStorage: Storage;

    beforeEach(() => {
        angular.mock.module('app');
        angular.mock.inject([
            'localStorage',
            function (_localStorage) {
                localStorage = _localStorage;
            }
        ]);
    });

    afterEach(() => {
        localStorage.clear();
    });

    it('can clear multiple values', () => {
        localStorage['thing one'] = 1;
        localStorage['thing two'] = 2;
        localStorage.clear();
        expect(localStorage.length).to.equal(0);
    });

    it('can set and get a value', () => {
        localStorage['first'] = 123;
        expect(localStorage['first']).to.equal('123');
    });

    it('has a length property', () => {
        expect(localStorage.length).to.equal(0);
        localStorage['first'] = 'Hello';
        localStorage['second'] = 'World';
        localStorage['third'] = 123;
        expect(localStorage.length).to.equal(3);
    });

});