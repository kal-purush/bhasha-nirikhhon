const puppeteer = require('puppeteer');
require('dotenv').config();

(async() => {
    const browser = await puppeteer.launch({headless: false});
    const page = await browser.newPage();
    await page.goto('https://unsplash.com/');

    await browser.close();
})();