const puppeteer = require('puppeteer');
var mysql = require('mysql');
const { link } = require('fs-extra');
const baseUrl = 'https://weworkremotely.com';
var con = mysql.createConnection({
    database: "6A46Hgkjgu",
    port: 3306,
    host: "remotemysql.com",
    user: "6A46Hgkjgu",
    password: "JI4TSwVn7E"
});

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

(async () => {
    const browser = await puppeteer.launch({
        args: ["--no-sandbox",
            "--disable-setuid-sandbox",],
    });
    const page = await browser.newPage();
    con.connect(function (err) {
        if (err) throw err;
        console.log("Connected!");
    });
    await page.goto('https://weworkremotely.com/categories/remote-programming-jobs#job-listings' + i, { timeout: 90000 });
    for (var i = 44; i <= 341; i++) {
        try {
            var startupUrl = null;
            const link = await page.evaluate("document.querySelector('#category-2 > article > ul > li:nth-child(" + i + ") > a').getAttribute('href')");
            const offerPage = await browser.newPage();
            await offerPage.goto(baseUrl + link, { timeout: 90000 });
            const position = await offerPage.evaluate(() => document.querySelector('body > div.content > div > div.listing-header-container > h1').textContent);
            const startupName = await offerPage.evaluate(() => document.querySelector('body > div.content > div > div.company-card > h2 > a').textContent);
            try {
                startupUrl = await offerPage.evaluate(() => document.querySelector('body > div.content > div > div.company-card > h3:nth-child(4) > a').getAttribute('href'));   
            } catch (error) {
                startupUrl = null;
            }
            const description = await offerPage.evaluate(() => document.querySelector("#job-listing-show-container").textContent);
            console.log('id : ' + (i-2));
            console.log('position : ' + position.trim());
            console.log('startup : ' + startupName.trim());
            console.log('StartupUrl : ' + startupUrl);
            console.log('description : ' + description);
            console.log('=================================================');
            await offerPage.close();
            con.query("SELECT id FROM startup where name=" + con.escape(startupName.trim()) + ";", function (err, result) {
                try {
                    idStartUp = JSON.parse(JSON.stringify(result))[0].id;
                } catch (error) {
                    if (startupUrl != null) {
                        con.query("INSERT INTO startup (name, website, sourceID) VALUES (" + con.escape(startupName.trim()) + "," + con.escape(startupUrl) + ",3);", function (err, result) {
                        })
                    }
                    else {
                        con.query("INSERT INTO startup (name, sourceID) VALUES (" + con.escape(startupName.trim()) + ",3);", function (err, result) {
                        })
                    }
                } finally {
                    con.query("SELECT id FROM startup where name=" + con.escape(startupName.trim()) + ";", function (err, result) {
                        idStartUp = JSON.parse(JSON.stringify(result))[0].id;
                        con.query("INSERT INTO offre (poste, travail, description , startupID ) VALUES (" + con.escape(position.trim()) + ",'Remote'," + con.escape(description.trim()) + "," + idStartUp + ");", function (err, result) {  
                        })
                    })
                }
            })
        } catch (error) {
            console.log(error);
            continue;
        }
    }
    await browser.close()

})();