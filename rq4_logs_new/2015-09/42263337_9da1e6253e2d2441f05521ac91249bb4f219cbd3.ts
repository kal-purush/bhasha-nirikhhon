/// <reference path="../../typings/tsd.d.ts" />


module tests.protractor {
    
    class HomePage {

        public data;
        
        public get = () : void => {
            browser.get('/');
            browser.waitForAngular();
            browser.wait(() => element(by.id('homePage')).isPresent());
            this.data = element.all(by.repeater('d in vm.data'));
        }
    }
    
    describe('myApp', function() {
        it('should load home page and show data list', function() {

            var homePage = new HomePage();

            homePage.get();

            expect(homePage.data.count()).toBe(1);
        });
    });
}