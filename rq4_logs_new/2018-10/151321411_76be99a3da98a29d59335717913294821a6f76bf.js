const puppeteer = require('puppeteer');

function delay(time) {
    return new Promise(function(resolve) { 
        setTimeout(resolve, time)
    });
 }

(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  await page.goto('https://vclock.com/');

  // Get the "viewport" of the page, as reported by the page.
  let dimensions = await page.evaluate(() => {
    return {
      width: document.documentElement.clientWidth,
      height: document.documentElement.clientHeight,
      deviceScaleFactor: window.devicePixelRatio,
      test: document.querySelector('#lbl-time').textContent
    };
  });

  console.log('Dimensions:', dimensions);

  await delay(20000);

  let dimensionss = await page.evaluate(() => {
    return {
      width: document.documentElement.clientWidth,
      height: document.documentElement.clientHeight,
      deviceScaleFactor: window.devicePixelRatio,
      test: document.querySelector('#lbl-time').textContent
    };
  });

  console.log('Dimensions:', dimensionss);

  await browser.close();
})();