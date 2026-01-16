/// <reference path="../../../typings/angular-protractor/angular-protractor.d.ts" />
/// <reference path="../../../typings/jasmine/jasmine.d.ts" />

/* jasmine specs for controllers go here */
describe('PhoneCat App', function() {

    describe('Phone list view', function(){

        beforeEach(function () {
            browser.get('index.html');
        });


        it('should filter the phone list as a user types into the search box', function() {

            var phoneList = element.all(by.repeater('phone in c.phones'));
            var query = element(by.model('c.query'));

            expect(phoneList.count()).toBe(3);
            query.sendKeys('nexus');
            expect(phoneList.count()).toBe(1);

            query.clear();
            query.sendKeys('motorola');
            expect(phoneList.count()).toBe(2);
        });

    });
});