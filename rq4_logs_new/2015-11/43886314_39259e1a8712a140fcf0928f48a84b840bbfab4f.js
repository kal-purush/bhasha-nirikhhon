// run with:
// casperjs test --ssl-protocol=any 2playerCorrect.js --username=gunnar & casperjs test --ssl-protocol=any 2playerCorrect.js --username=blaine


var turnOnDebug = require('../debug');
var configureCasper = require('../configure');
var waitAndClick = require('../helpers').waitAndClick;
var checkForText = require('../helpers').checkForText;
var checkForSelector = require('../helpers').checkForSelector;


url = 'http://localhost:3000';

// Returns a random integer between min (included) and max (included)
// Using Math.round() will give you a non-uniform distribution!
function getRandomIntInclusive(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

casper.test.begin('After 3 correct clicks, scores should be 3', 4, function (test) {

    var username = casper.cli.get("username");


    casper.start();

    configureCasper(casper);
    turnOnDebug(casper);

    casper.thenOpen(url);

    casper.wait(1000);

    casper.then(function () {
        // if logged in, log out before logging in
        if (this.exists('#login-name-link')) {
            this.echo('++++ found #login-name-link');
            this.click('#login-name-link');
            waitAndClick('#login-buttons-logout', this);
        }
    });

    casper.then(function () {
        waitAndClick('#login-sign-in-link', this);
    });

    casper.then(function() {
        this.echo('++++ going to wait for username/password field');
        this.waitForSelector('#login-buttons-password', function () {
            this.echo('++++ See #login-buttons-password');
        });
    });

    casper.thenEvaluate(function(username, password) {
        document.querySelector('#login-username-or-email').value = username;
        document.querySelector('#login-password').value = password;
        // This not working:
        //document.querySelector('#login-buttons-password').click();
    }, username, 'password');

    casper.thenClick('#login-buttons-password');

    casper.then(function () {
        checkForText(username, this);
    });

    casper.then(function () {
        if ("button[id='#start-button']:enabled") {
            this.echo('==== found enabled start button');
            this.click('#start-button');
        }
    });

    casper.then(function () {
        checkForText("Waiting", this);
    });


    casper.then(function () {
        waitAndClick('#join-button', this);
    });

    casper.then(function () {
        checkForText("Game ends in", this);
    });


    casper.then(function () {
        var selector = "button[id='bot-button']:disabled";
        casper.wait(1000, function () {
            checkForSelector(selector, this);
        });
    });

    casper.then(function () {
        casper.wait(1000, function () {
            this.capture(username + '.png');
        });
    });

    casper.run(function () {
        test.done();
    });
});