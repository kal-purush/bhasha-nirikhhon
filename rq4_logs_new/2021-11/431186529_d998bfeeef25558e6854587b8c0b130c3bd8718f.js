const puppeteer = require("puppeteer");
const fs = require('fs');
//const { stringify } = require("querystring");
//require("dotenv").config();

const url = 'https://www.boxofficemojo.com/chart/top_lifetime_gross/?area=XWW';

(async () => {
 
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    
    await page.goto(url, {"waitUntil" : "networkidle2"});

    const movies = await page.evaluate(() => {
        // stocke les données dans des array
        let rank = Array.from(document.querySelectorAll('tr td.mojo-field-type-rank')).map(el=>el.textContent);
        let movie = Array.from(document.querySelectorAll('tr td.mojo-field-type-title a.a-link-normal')).map(el=>el.textContent);
        let boxOffice = Array.from(document.querySelectorAll('tr td.mojo-field-type-money')).map(el=>el.textContent);
        let year = Array.from(document.querySelectorAll('tr td.mojo-field-type-year a.a-link-normal')).map(el=>el.textContent);
        
        // map le premier tableau et réccupère le boxoffice + year en fonction de son index
        const data = rank.map((rank, index) => ({
            rank: rank, 
            title: movie[index], 
            boxOffice: boxOffice[index], 
            year: year[index]
        }))
        return {
            data
        }
    })    
    
    // ajout des infos dans un json
    const data = JSON.stringify(movies);
    fs.writeFile('./data.json', data, (err) => {
        if (err) {
            throw err;
        }
        console.log("JSON data is saved.");
    });

    debugger;
    
    await browser.close();
   })();