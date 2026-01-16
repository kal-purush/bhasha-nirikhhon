import * as fs from "fs";
import { join } from 'path'

const androidAppName: string = 'Sauce_labs.apk';
const iosAppName: string = 'ios_app';

const ANDROID_CAPABILITIES = [
    {
        "appium:platformName": "Android",
        "appium:deviceName": "Pixel 6 Pro API 32",
        // "appium:app": join(process.cwd(), '/app/android/Sauce_labs.apk'),
        "appium:automationName": "UiAutomator2",
        "appium:udid": "emulator-5554",
        "appium:chromedriverExecutable": `${process.cwd()}/chromedriver-mobile/chromedriver`,
        'appium:app': `${process.cwd()}/app/android/${androidAppName}`,
        // 'appium:chromedriverExecutable': `${process.cwd()}/app/chromedriver`	       
        // "appium:noReset": true
    }
];

const IOS_CAPABILITIES = [
    {
        "platformName": "iOS",
        "appium:deviceName": "iPhone 15",
        "appium:automationName": "XCUITest",
        "appium:platformVersion": "17.2",
        // "appium:app": "/Users/testvagrant/Documents/ts_wdio/app/ios/ios_app",
        'appium:app': `${process.cwd()}/app/ios/${iosAppName}`,
    }
];

exports.config = {
    runner: "local",
    port: 4723,
    specs: [`${process.cwd()}/test/spec/**/*.test.ts`],
    capabilities: process.env.PLATFORM === "ANDROID" ? ANDROID_CAPABILITIES : IOS_CAPABILITIES,
    maxInstances: 1,
    logLevel: "info",
    waitforTimeout: 30000,
    connectionRetryTimeout: 120000,
    connectionRetryCount: 3,
    services: ["appium"],
    framework: "mocha",
    reporters: [
        "spec",
        [
            "allure",
            {
                outputDir: "allure-results",
                disableWebdriverStepsReporting: true,
                disableWebdriverScreenshotsReporting: false,
                disableMochaHooks: true
            },
        ],
    ],
    mochaOpts: {
        ui: "bdd",
        timeout: 60000,
    },
    afterTest: async function (test, context, { error, result, duration, passed, retries }) {
        if (!fs.existsSync("./errorShots")) {
            fs.mkdirSync("./errorShots");
        }
        if (!passed) {
            await driver.saveScreenshot(`./errorShots/${test.title.replaceAll(" ", "_")}.png`);
        }
    }
};