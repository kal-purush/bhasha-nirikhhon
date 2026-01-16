import * as express from 'express';
import {realtorRequest, redfinRequest, truliaRequest} from './controllers';
import {rejectPromise} from '../util/rejectPromise';
const request = require('request');

const app = express();

app.set('port', 3000);

app.get('/propertyPhoto/:address', realtorRequest);
app.get('/propertyPhoto1/:address', redfinRequest);
app.get('/propertyPhoto2/:address', truliaRequest);


function makeZillowRequest(address) {
    const zilUrl = `https://www.zillow.com/homes/${address}_rb/`;
    console.log(zilUrl);

    const options = {
        method: 'GET',
        url: 'https://www.zillow.com/homes/2248+w+230th_rb/',
        headers:
        {
            cookie: 'zgsession=1|378a942f-2184-4563-94f1-814ac45578e5; zguid=24|%24d84afb5a-900d-4e20-8a67-935ff842b2d5; abtest=3|DOjqyTP_AMMxPSaQ8w; pxvid=ad344f60-3abc-11e9-93d0-9b44f4fa3e50; _pxvid=ad344f60-3abc-11e9-93d0-9b44f4fa3e50; ajs_user_id=null; ajs_group_id=null; ajs_anonymous_id=%22d84afb5a-900d-4e20-8a67-935ff842b2d5%22; G_ENABLED_IDPS=google; JSESSIONID=54CEF49F78BD1A30E15A31B30F3BABF2; _pxff_tm=1; search=6|1554568957213%7Crect%3D33.830052%252C-118.310137%252C33.80224%252C-118.333097%26zm%3D14%26disp%3Dmap%26mdm%3Dauto%26p%3D1%26z%3D1%26fs%3D1%26fr%3D0%26mmm%3D1%26rs%3D0%26ah%3D0%26singlestory%3D0%09%01%0996159%09%09%092%090%09US_%09; AWSALB=P3XeChNhedsH2o08PcoF0Sb2xR7tnrH0IElgj6sK7hDtAKg62qBb+OVus6VRSu35fAdY3s8CuBC4vut2pISZ5QYgDUC9ff7WAWhrISnyP9+4ISYvdtAH72C9oSkZ; _px3=eee020384ec3a37a1f482f901f924db45484e5383becd0fa78c374f4f86704a8:Y1BjGgQu9aEFys8BGGkgr+C1KeegWTYf+7EPyoaBh70D5gZeVy33v7r5Pwy4q858UOiQriA9sUZzjZsQ0NZ2zw==:1000:wCG5HEOWvURDapg8MJHmcDr125bs4ySyU63HI7xdIUJTNCoCZ2x19aT3S0+VobLVUY1qYVNKac3+qJWRDYkm8dFc7SobS/3u7Wn9avXuw/lVbnTQY6/YPU0NmvjuRVpQ8GS/LvdmOnZkf4HgZmTV186nTaRgjntWqHITnis1NeE=',
            // 'Postman-Token': 'dd9659d2-8d8e-44b8-8b21-c2a47d9ab4fe',
            // 'accept-language': 'en-US,en;q=0.9',
            // 'accept-encoding': 'gzip, deflate, br',
            accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36',
            'upgrade-insecure-requests': '1',
            'cache-control': 'max-age=0,no-cache',
            authority: 'www.zillow.com'
        }
    };


    return request(options).then( body => {
        const imgUrl = cheerio('.photo-tile-image', body).attr().src;
        return imgUrl;
    }).then(body => {
        const imgUrl = cheerio('.photo-tile-image', body).attr().src;
        return imgUrl;
    });

    // return new Promise(resolve => {
    //     request(options, function (err, response, body) {
    //         // request({ url: zilUrl }, function (err, response, body) {
    //         if (err) { console.log(err); return; }
    //         console.log(body);
    //         resolve(body);
    //     })
    // }).then(body => {
    //     var imgUrl = cheerio('.photo-tile-image', body).attr().src;
    //     return imgUrl;
    // })
}


function makeHomeSnapRequest(address): Promise<string> {
    const homeSnapUrl = 'https://www.homesnap.com/service/Misc/Search';
    let homeSnapImgUrl = 'https://www.homesnap.com/';

    const body = JSON.stringify({
        text: address,
        polygonType: 1,
        skip: 0,
        take: 8,
        submit: false
    });

    const options = {
        method: 'POST',
        url: homeSnapUrl,
        headers: {
            Connection: 'keep-alive',
            'X-Requested-With': 'XMLHttpRequest',
            Referer: 'https://www.homesnap.com/',
            Accept: 'application/json, text/javascript, */*; q=0.01',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            Origin: 'https://www.homesnap.com'
        },
        body
    };

    return request(options).then(body => {
        const jsonObject = JSON.parse(<any>body);
        try { homeSnapImgUrl = homeSnapImgUrl + jsonObject.d.Properties[0].Url; }
        catch (err) { return rejectPromise('address not found'); }
        return homeSnapImgUrl;
    });
}


// Unable to grab the image due to the HTML response not consisting of any img tags
// Page that is returned is fairly blank with a checkbox and some buttons (check actual response)
function getHomeSnapImgUrl(homeSnapUrl): Promise<string> {
    const options = {
        method: 'GET',
        url: homeSnapUrl,
        headers: {
            Connection: 'close',
            'X-Requested-With': 'XMLHttpRequest',
            Referer: 'https://www.homesnap.com/',
            Accept: 'application/json, text/javascript, */*; q=0.01',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            Origin: 'https://www.homesnap.com'
        }
    };
    return request(options).then(body => {
        const imgUrl = cheerio('.listingImage large', <any>body);
        if (!imgUrl) { return rejectPromise('image not found'); }
        return imgUrl;
    });
}

export = app;