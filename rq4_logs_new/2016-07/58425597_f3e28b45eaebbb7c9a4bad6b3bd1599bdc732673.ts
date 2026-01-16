import {Callback, expect} from '../support/config';
import {UsersPage} from '../pages/users.po';

export = function() {

  let page = new UsersPage();

  this.Given(/^I am not authenticated$/, (callback:Callback) => {
    callback();
  });

  this.When(/^I try to access some restricted feature$/, (callback:Callback) => {
    page.goToUsers();
    callback();
  });

  this.Then(/^I should be redirected to the 'Sign In' page$/, (callback:Callback) => {
    expect(page.getGreetings()).to.eventually.equal('Sign In!').and.notify(callback);
  });
};