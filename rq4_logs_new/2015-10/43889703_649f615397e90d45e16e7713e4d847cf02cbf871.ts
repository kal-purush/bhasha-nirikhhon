import chai = require('chai');

export = function() {

    var expect = chai.expect;
    var bottle, Bottle, currentPhrase;

    this.Before((scenario, callback) => {
        Bottle = require("../app").Bottles;
        callback();
    });

    this.Given(/^I have (\d+) bottles of beer$/, (bottles, callback) => {
        bottle = new Bottle(bottles);
        callback();
    });

    this.When(/^I sing the (first|second) paragraph$/, (paragraph, callback) => {
        currentPhrase = bottle.phraseFor(paragraph);
        callback();
    });

    this.Then(/^I should say "([^"]*)"$/, (phrase, callback) => {
        expect(currentPhrase).to.be.equal(phrase);
        callback();
    });
};