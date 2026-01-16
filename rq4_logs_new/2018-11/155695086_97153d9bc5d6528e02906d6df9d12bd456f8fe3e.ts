import { ProtractorBrowser, Config } from 'protractor';
export let config: Config = {
    seleniumAddress: 'http://localhost:4444/wd/hub',
    capabilities: {
        'browserName': 'chrome'
    },
    framework: 'jasmine',
    specs: ['./src/app/**/*.spec.js'],
    jasmineNodeOpts: {
        defaultTimeoutInterval: 90000
    },
    onPrepare: () => {
        let globals = require('protractor');
        let browser = globals.browser;
        browser.manage().window().maximize();
        browser.manage().timeouts().implicitlyWait(5000);

        browser.getProcessedConfig().then(function (config) {
            var fullName = config.specs[0];
            var fileName = fullName.substring(fullName.lastIndexOf('/') + 1);
            console.log('fileName:', fileName);
        });
    }
}