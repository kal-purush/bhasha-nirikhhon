var assert = require('assert'),
    fs = require('fs'),
    test = require('selenium-webdriver/testing'),
    webdriver = require('selenium-webdriver');

test.describe('TrendMapper Application', function() {
  this.timeout(15000);
  var driver;

  test.before(function() {
    driver = new webdriver.Builder()
      .withCapabilities(webdriver.Capabilities.chrome())
      .build();
  });

  test.it('Should load the front page', function() {
    driver.get('http://localhost:3000');
  });

  test.after(function() {
    driver.quit();
  });
});