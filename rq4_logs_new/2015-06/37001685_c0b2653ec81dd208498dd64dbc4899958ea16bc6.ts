/// <reference path="../../../typings/angularjs/angular-mocks.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

/* jasmine specs for controllers go here */
describe('service', function() {
    beforeEach(module('PhonecatApp'));

    it('check the existence of Phone factory', inject(function(Phone) {
        expect(Phone).toBeDefined();
    }));
});