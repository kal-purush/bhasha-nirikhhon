import puppeteer from 'puppeteer';
// Or import puppeteer from 'puppeteer-core';

// Launch the browser and open a new blank page
const browser = await puppeteer.launch({headless: false});
const page = await browser.newPage();

// DEMO
const mapuri = "https://maps.app.goo.gl/bi1eU7xkQYeq1xZb8"

const orderOnlineSelector = 'a[href^="https://www.google.com/viewer/chooseprovider'
const providerSelector = 'div[ssk="23:2Storefront by DoorDash"] > a'


const menuItemSelector = 'div[data-anchor-id="MenuItem"]'
const nameSelector = 'h3[data-telemetry-id="storeMenuItem.title"]'
const descSelector = 'span[data-telemetry-id="storeMenuItem.subtitle"]'

await page.setViewport({width: 1080, height: 1080});
// Navigate the page to a URL.

await page.setRequestInterception(true);
page.on('request', (req) => {
  if(req.resourceType() === 'image') { req.abort();}
  else { req.continue(); }
});

if (false){
    try {
        await page.goto(mapuri);
    } catch (error) {
        throw {name: "LINK_NOT_ACCESSIBLE", message: "There was a problem accessing the URL: " + mapUri}; 
    }
    
    await page.waitForNavigation({waitUntil: 'domcontentloaded'});
    // console.log("Took", Date.now() - mapstart, " ms to load the Google Maps DOM", mapuri);
    await page.waitForSelector(orderOnlineSelector);
    await page.click(orderOnlineSelector);
    await page.waitForNavigation({waitUntil: 'domcontentloaded'});
    
    // console.log("Took", Date.now() - mapstart, " ms to load the Google Maps DOM", mapuri);
    await page.waitForSelector(providerSelector);
    await page.click(providerSelector);
    await page.waitForNavigation({waitUntil: 'domcontentloaded'});
    
}

await page.goto("https://order.online/store/neirs-tavern-woodhaven-732524/?hideModal=true&pickup=true&rwg_token=AJKvS9UWLLRzKA97VepY72JweA-ssHpzy7lS_TSwqZMGNknyjcRFqMZnVQF5sgAq-rKchHS7Wv1uJy6mYkAxi0n1F3DJ2Qw8vA==&utm_source=gfo");
await page.waitForNavigation({waitUntil: 'domcontentloaded'});

const menuItems = await page.evaluate(async (menuItemSelector, nameSelector, descSelector) => {
    return new Promise((resolve, reject) => {
        const distance = 140 * 4.5;
        var heightObject = { totalHeight: 0};
        var result = [];
        
        const timer = setInterval(() => {
            if (heightObject.totalHeight >= document.body.scrollHeight) {
                clearInterval(timer);
                console.log("complte!", result);
                resolve(result);
            } else {
                window.scrollBy(0, distance);
                heightObject.totalHeight += distance;
                
                console.log("selecting items: ", menuItemSelector);
                const productCollection = document.querySelectorAll(menuItemSelector);
                
                console.log("queried:", productCollection);
                productCollection.forEach((product) => {
                    const itemName = product.querySelector(nameSelector)?.innerText;
                    const itemDescription = product.querySelector(descSelector)?.innerText;                
                    result.push({name : itemName, desc: itemDescription});
                })
            }
        }, 100);
    })
}, menuItemSelector, nameSelector, descSelector )

console.log(menuItems);

// console.log(items);


// await page.setViewport({width: 1080, height: page_height});

// await browser.close();