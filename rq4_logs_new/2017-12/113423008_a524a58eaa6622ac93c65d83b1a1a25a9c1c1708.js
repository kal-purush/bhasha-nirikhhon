exports.config = {
  framework: 'jasmine',
  seleniumAddress:'http://localhost:4444/wd/hub',
  specs: ['spec.js'],
  capabilities: {
    browserName: 'chrome',
    chromeOptions: {
      args: [ "--headless", "--disable-gpu", "--window-size=800x600" ]
    }
  }
}