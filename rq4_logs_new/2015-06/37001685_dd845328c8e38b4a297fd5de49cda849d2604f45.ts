/// <reference path="../../../typings/angularjs/angular-mocks.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

/* jasmine specs for controllers go here */
describe('filter', function() {
    beforeEach(module('phonecatFilters'));

    describe('checkmark', function() {
        it('should convert boolean values to unicode checkmark or cross',
            inject(function(checkmarkFilter: any) {
                expect(checkmarkFilter(true)).toBe('\u2713');
                expect(checkmarkFilter(false)).toBe('\u2718');
            })
        );
    });
});