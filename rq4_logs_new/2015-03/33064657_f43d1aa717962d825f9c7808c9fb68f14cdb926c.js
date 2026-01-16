module.exports = {
  beforeEach : function(browser) {
    browser
      .url("http://homework-vehicle.s3-website-us-west-1.amazonaws.com/")
      .waitForElementVisible('body', 1000)
  },
  "Application exists" : function (browser) {
    browser
      .assert.containsText('h2', 'Vehicle')
      .end();
  },
  "Can make a vehicle" : function (browser) {
    browser
      .setValue('#vehicle-name', 'Honda Accord')
      .click('.dropdown')
      .click('#car-select')
      .click('#add-button')
      .waitForElementVisible('#vehicle-0', 2000)
      .assert.containsText('.vehicle-generated-name', 'Honda Accord')
      .end();
  },
  "Info button shows vehicle details" : function (browser) {
    browser
      .setValue('#vehicle-name', 'Ford Taurus')
      .click('.dropdown')
      .click('#car-select')
      .click('.add-button')
      .click('.info-button')
      .assert.containsText('#vehicle-info-type', 'car' )
      .assert.containsText('#vehicle-info-name', 'Ford Taurus' )
      .assert.containsText('#vehicle-info-seats', '4')
      .assert.containsText('#vehicle-info-wheels', '4')
      .end();
  },
  "Making a car" : function (browser) {
    browser
      .setValue('#vehicle-name', 'Ford Taurus')
      .click('.dropdown')
      .click('#car-select')
      .click('.add-button')
      .click('.info-button')
      .assert.containsText('#vehicle-info-type', 'car' )
      .assert.containsText('#vehicle-info-name', 'Ford Taurus' )
      .assert.containsText('#vehicle-info-seats', '4')
      .assert.containsText('#vehicle-info-wheels', '4')
      .end();
  },
  "Making a motorcycle" : function (browser) {
    browser
      .setValue('#vehicle-name', 'Kawasaki Ninja')
      .click('.dropdown')
      .click('#motorcycle-select')
      .click('.add-button')
      .click('.info-button')
      .assert.containsText('#vehicle-info-type', 'motorcycle' )
      .assert.containsText('#vehicle-info-name', 'Kawasaki Ninja' )
      .assert.containsText('#vehicle-info-seats', '1')
      .assert.containsText('#vehicle-info-wheels', '2')
      .end();
  },

  "Submit only occurs if all parameters are fulfilled" : function(browser) {
    browser
    .assert.hidden('#error-message')
    .click('.add-button')
    .assert.visible('#error-message')
    .assert.containsText('#error-message', 'All fields are required.' )
    .end();
  }
};