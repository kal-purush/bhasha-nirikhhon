'use strict';
const express       = require('express');
const bodyParser    = require('body-parser');
const puppeteer     = require('puppeteer');
const fs            = require('fs');
const crypto        = require('crypto')
const app           = express();
const pcUserAgent   = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36';
const pcViewport    = {width: 1034, height: 500};
const spUserAgent   = 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1';
const spViewport    = {width: 375, height: 500};

var ssResult;

app.use(express.static('/app'));
app.listen(80);

app.get('/', function(req, res) {
    res.send('GET request to the web-site.');
});

app.use(bodyParser.urlencoded({extended: true}));

function md5hex(str) {
  const md5 = crypto.createHash('md5')
  return md5.update(str, 'binary').digest('hex')
}

async function ss (url, userAgent, viewport) {
    console.log('=== start ===');
    var browser = await puppeteer.launch({
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage'
        ]
    });
    var tmpFile = md5hex(url) + '.png';
    var page = await browser.newPage();
    page.setUserAgent(userAgent);
    page.setViewport(viewport);
    await page.goto(url, {waitUntil: 'networkidle2'});
    await page.screenshot({
        path: tmpFile,
        fullPage: true
    });
    browser.close();
    var v = fs.readFileSync(tmpFile);
    console.log('=== end ===');
    ssResult = v;
}

app.post('/getScreenShot.php', async (req, res)=>{
    // リクエストボディを出力
    var url = req.body.url;
    await ss(url, pcUserAgent, pcViewport);
//  await ss(url, spUserAgent, spViewport);
    res.writeHead(200,{"Content-Type":"image/png"});
    res.end(ssResult);
});