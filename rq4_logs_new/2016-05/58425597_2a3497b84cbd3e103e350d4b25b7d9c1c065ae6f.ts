import {Callback, expect} from '../support/config';
import {HomePage} from '../pages/home.po';

export = function() {

  let page = new HomePage();

  this.Given(/^I am not authenticated$/, (callback:Callback) => {
    callback();
  });

  this.When(/^I go to Home$/, (callback:Callback) => {
    page.goToHome();
    callback();
  });

  this.Then(/^I should see the text 'Equalizei rocks!'$/, (callback:Callback) => {
    expect(page.getGreetings()).to.eventually.equal('Equalizei rocks!').and.notify(callback);
  });
};