/// <reference path="../../../../typings/meteor/node.d.ts"/>
/// <reference path="../../../../typings/meteor-cucumber/meteor-cucumber.d.ts"/>
import mc = require('meteor-cucumber');
import url = require('url');

'use strict';
function defineSteps() {
  var self = <mc.StepDefinitions>this;
  // You can use normal require here, cucumber is NOT run in a Meteor context (by design)

  self.Given(/^I am a new user$/, function () {
    var self = <mc.World>this;
    // no callbacks! DDP has been promisified so you can just return it
    return self.ddp.callAsync('reset', []); // this.ddp is a connection to the mirror
  });

  self.When(/^I navigate to "([^"]*)"$/, function (relativePath) {
    var self = <mc.World>this;
    // WebdriverIO supports Promises/A+ out the box, so you can return that too
    return self.browser. // this.browser is a pre-configured WebdriverIO + PhantomJS instance
      url(url.resolve(process.env.HOST, relativePath)); // process.env.HOST always points to the mirror
  });

  self.Then(/^I should see the title "([^"]*)"$/, function (expectedTitle) {
    var self = <mc.World>this;      // you can use chai-as-promised in step definitions also
    return self.browser.
      waitForVisible('h1'). // WebdriverIO chain-able promise magic
      getTitle().should.become(expectedTitle);
  });
}

export = defineSteps();