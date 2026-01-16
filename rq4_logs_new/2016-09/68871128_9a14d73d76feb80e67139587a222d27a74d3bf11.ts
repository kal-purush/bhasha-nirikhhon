import {t, /*selectDropdownByValue*/} from '../../../frameworks/test/index';

declare var browser: any, element: any, by: any;

t.describe('gv-home', function() {

  t.be(function() {
    browser.get('/gv-home');
  });

  t.it('should have correct h2', function() {
      t.e(element(by.css('sd-app gv-home h2')).getText()).toEqual('I love technology!');
  });

});