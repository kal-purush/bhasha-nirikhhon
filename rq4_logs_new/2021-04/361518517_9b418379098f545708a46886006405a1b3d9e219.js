//@ts-check
const {firefox} = require('playwright');
const fs = require('fs');
var TurndownService = require('turndown');

async function grab(turndownService, context, page) {

    if (fs.existsSync('bruvax')) {
        deleteFolderRecursive('bruvax');
    }

    var searchUrl = `https://bruvax.brussels.doctena.be/`;

    await page.goto(searchUrl);

    const article = page.$$('section article')
    await article.screenshot({path: 'bruvax/screenshot.png'});

    let innerHtml = await article.innerHTML();
    fs.writeFileSync('bruvax/article.md', turndownService.turndown(`<div><div>${innerHtml}</div><img src="screenshot.png"><p><a href="${searchUrl}">Based on this search</a></p></div>`));
}

// because we're using an old node version on github actions...
function deleteFolderRecursive(path) {
    let files = [];
    if (fs.existsSync(path)) {
        files = fs.readdirSync(path);
        files.forEach(function (file, index) {
            let curPath = path + "/" + file;
            if (fs.lstatSync(curPath).isDirectory()) { // recurse
                deleteFolderRecursive(curPath);
            } else { // delete file
                fs.unlinkSync(curPath);
            }
        });
        fs.rmdirSync(path);
    }
};

(async () => {

    console.log("Starting...")

    var turndownService = new TurndownService({emDelimiter: '*'}).remove('script');

    let browser = null;
    try {
        browser = await firefox.launch();
        const context = await browser.newContext();
        const page = await context.newPage();

        await grab(turndownService, context, page);

    } catch (e) {
        console.error("Something failed", e);
        return process.exit(1);
    } finally {
        if (browser != null) {
            await browser.close();
        }
        console.log("Finished.");
    }
})();