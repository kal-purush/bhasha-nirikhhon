const {Builder, By, Key, until} = require('selenium-webdriver');
const chrome = require('selenium-webdriver/chrome');

async function exampleTest() {
    let driver = await new Builder().forBrowser('chrome').setChromeOptions(new chrome.Options()).build();
    try {
        // Navigate to a web page
        await driver.get('http://localhost:3000');
        // Perform actions on the web page
        let title = await driver.getTitle();
        console.log('Title is:', title);
        // Assertions or other test operations can go here
    } finally {
        await driver.quit();
    }
}

exampleTest();