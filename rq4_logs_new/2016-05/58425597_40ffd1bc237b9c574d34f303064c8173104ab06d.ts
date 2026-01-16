import {CallbackStepDefinition} from 'cucumber';
import {HomePage} from '../pages/home.po';
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';

chai.use(chaiAsPromised);

export = function() {

  let page = new HomePage();
  let expect = chai.expect;

  this.Given(/^I am not authenticated$/, (callback:CallbackStepDefinition) => {
    callback();
  });

  this.When(/^I go to Home$/, (callback:CallbackStepDefinition) => {
    page.goToHome();
    callback();
  });

  this.Then(/^I should see the text 'Equalizei rocks!'$/, (callback:CallbackStepDefinition) => {
    expect(page.getGreetings()).to.eventually.equal('Equalizei rocks!').and.notify(callback);
  });
};