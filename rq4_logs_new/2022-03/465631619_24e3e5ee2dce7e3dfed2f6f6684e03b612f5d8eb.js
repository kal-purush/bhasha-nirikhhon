//Import Puppeteer after NPM install:
const puppeteer = require('puppeteer');
//Import File system save feature:
const fs = require('fs/promises')

//Async function call to start Puppeteer and to grab information off of the website:
async function start() {
    const browser = await puppeteer.launch()
    const page = await browser.newPage()
    await page.screenshot({
        path: "screenshot.png", fullPage: true
    })
    await page.goto('https://en.wikipedia.org/wiki/Godzilla')

    //This writes all of the actors from the table.info box element to a godzillaSummary text file.
    const godzillaSummary = await page.evaluate(() => {
        return Array.from(document.querySelectorAll(`.infobox`)).map(y => y.textContent)
    })
    await fs.writeFile('godzillaSummary.txt', godzillaSummary.join("\r\n"))

    //This writes all of the paragraphs from the Godzilla Wikipedia page to a text file called godzilla.text
    const godzillaPage = await page.evaluate(() => {
        return Array.from(document.querySelectorAll(`p`)).map(x => x.textContent)
    })
    await fs.writeFile('godzilla.txt', godzillaPage.join("\r\n"))


    //Get a picture from the site by selecting multiple elements using the $$eval method:
    const photos = await page.$$eval("img", (imgs) => {
        return imgs.map(x => x.src)
    })

    //Loop through the img array using a for-of loop:
    for (const photo of photos) {
        const imagePage = await page.goto(photo)
        await fs.writeFile(photo.split("/").pop(), await imagePage.buffer())
    }


    await browser.close()

}

//Set interval to automatically scrap site every 500000 seconds:
// setInterval(start, 50000)

start();