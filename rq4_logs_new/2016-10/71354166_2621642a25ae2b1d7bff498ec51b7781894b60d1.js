
exports.config = {
  
  //sauceUser: process.env.SAUCE_USERNAME,
  //sauceKey: process.env.SAUCE_ACCESS_KEY,
  
  sauceUser: "satwell93",
  sauceKey: "d4ed861b-e746-47b7-bcf7-4e954926aa71",
  
  
  //seleniumAddress: 'http://localhost:4444/wd/hub',
  //seleniumAddress: 'http://ondemand.saucelabs.com:80/wd/hub',
  
  specs: ['specs.js'],
  
  capabilities: {
    'browserName': 'chrome',
    'version': '41',
    'platform': 'Windows 7',
    'name': "chrome-tests"
  }
  
  
  
  
  
}