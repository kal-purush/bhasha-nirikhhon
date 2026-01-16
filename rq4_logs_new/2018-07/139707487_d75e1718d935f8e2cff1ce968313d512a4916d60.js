const puppeteer = require('puppeteer');
require('dotenv').config();

class ListPageScrapper {
  constructor(args) {
    this.pageID = args.pageID;
  }
  async getListPageStatus() {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    const wait = ms => {
      return new Promise(resolve => setTimeout(() => resolve(), ms));
    }

    try {
      await page.goto('https://www.instagram.com/' + `${this.pageID}` + '/');

      let toSingleElements = await page.$$('.Nnq7C');
      let postRowCount = 1;
      let morePosts = true;
      let toSingleHrefsArray = [];
      while (morePosts) {
        toSingleHrefsArray.push(await page.$eval(`.Nnq7C:nth-child(${1}) > .v1Nh3:nth-child(${1}) > a`, el => el.href));
        toSingleHrefsArray.push(await page.$eval(`.Nnq7C:nth-child(${1}) > .v1Nh3:nth-child(${2}) > a`, el => el.href));
        toSingleHrefsArray.push(await page.$eval(`.Nnq7C:nth-child(${1}) > .v1Nh3:nth-child(${3}) > a`, el => el.href));
        /*if(postRowCount % 4 == 0) {
          await page.evaluate(() => {
            window.scrollBy(0, 2000);
          })
          await wait(2000);
        }*/
        await page.evaluate(() => { window.scrollBy(0, 300) });
        await wait(2000);
        postRowCount++;
        if(postRowCount === 50) {
          morePosts = false;
        }
      }

      await browser.close();

      return toSingleHrefsArray;
    } catch (e) {
      console.error(e);
      await browser.close();
      return e;
    }
  }
}

class SingleScrapper {
  constructor(args) {
    this.roomURL = args.roomURL;
  }

  async getSingleStatus() {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    try {
      await page.goto(this.roomURL);

      let singleInfoProperty = await page.evaluate(() => {
        let singleInfomationsElement = document.querySelectorAll('.Xl2Pu > .gElp9 > span > a');
        let singleInfomations = [singleInfomationsElement.length];
        let singleInfomationsKey = '';
        let singleInfomationsValue = '';
        let loopCount = 0;
        for (let i=0; i < singleInfomationsElement.length; i++) {
          singleInfomations[i] = singleInfomationsElement[i].innerText;
          loopCount = singleInfomations;
        }
        return loopCount;
      });

      await browser.close();

      return singleInfoProperty;
    } catch (e) {
      console.error(e);
      await browser.close();
      return e;
    }
  }
}

const listpageInfo = new ListPageScrapper({
  pageID: 'furuzyo',
});

/*listpageInfo.getListPageStatus().then((listpageLinks) => {
  let singleInfos = new Array(listpageLinks.length);
  for(let i=0; i < listpageLinks.length; i++) {
    const singleInfo = new SingleScrapper({
      roomURL: listpageLinks[i]
    });
    singleInfo.getSingleStatus().then((result) => {
      singleInfos[i] = result;
    console.log(singleInfos[i]);
    });
  }
});*/

listpageInfo.getListPageStatus().then(result => {
  let output = Array.from(new Set(result));
  console.log(output);
});