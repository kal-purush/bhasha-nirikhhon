// conf.js

exports.config = {
    framework: 'jasmine',

    seleniumServerJar: process.env[(process.platform == 'win32') ? 'APPDATA' : 'NODE_PATH'] + '/npm/node_modules/protractor/node_modules/webdriver-manager/selenium/selenium-server-standalone-2.53.1.jar',
    seleniumPort: null,

    suites: {
        //TODO: add here other suites
        exercise1 : './exercise1/*solution.js',
        exercise2 : './exercise2/*solution.js'
    },

    onPrepare: function() {
        //TODO: add prepare statements
        browser.driver.manage().window().setSize(800, 800);
    },

    params: {
        //TODO: add global params here
        exercise1: {
            url: 'http://pfy.cloud.reply.eu/protractor/ex1/'
        },
        exercise2: {
            url: 'http://pfy.cloud.reply.eu/protractor/ex2/'
        }
    },

    capabilities: {
        browserName: 'chrome',

        chromeOptions: {
            args: [
                '--disable-extensions'
            ]
        }
    }

};