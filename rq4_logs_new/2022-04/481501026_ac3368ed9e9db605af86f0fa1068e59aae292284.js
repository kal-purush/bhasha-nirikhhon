'use strict';
const
    config = require('config'),
    express = require('express'),
    request = require('request');
const { Console } = require('console');
const puppeteer = require('puppeteer');

    
const fs = require('fs');
var path = require('path');
var Chart = require('chart.js');

var app = express();
var port = process.env.PORT || process.env.port || 5000;
app.set('port', port);
app.use(express.json());
app.listen(app.get('port'), function(){
    console.log('[app.listen] Node app is running on port', app.get('port'));
});
module.exports = app;

// const SHEETDB_PRODUCTINFO_ID = config.get('productinfo_id');

app.post('/webhook', async(req, res) =>{
    console.log("[Webhook] in");
    let data = req.body;
    let plotType = data.queryResult.parameters["Plot"];
    let stockID = data.queryResult.parameters["StockID"];

    // 兩種狀況 P / K
    // P : 當日圖
    // K : 三個月的K線圖
    if(plotType == "P"){
        // plot p
    }else if(plotType == "K"){
        var urlStock;
        (async function main() {
            try {
                console.log("[Chart] in")
                const browser = await puppeteer.launch({
                    headless:true,
                    args: [
                        '-no-sandbox',
                        '-disable-setuid-sandbox'
                        ,'-disable-gpu',
                        '-disable-dev-shm-usage',
                        '-no-first-run',
                        '-no-zygote',
                        '-single-process'
                    ]
                    });
                const [page] = await browser.pages();
                await page.goto("https://stock-chart-buzz.herokuapp.com/chart/" + stockID, { waitUntil: 'networkidle0' });
                const data = await page.content();
                // console.log(data);
                urlStock = data.substring(data.indexOf("https://cdn.anychart.com/shared/anychart"), data.indexOf("</p>"))
                await browser.close();
                console.log("[Chart] out")
            } catch (err) {
              console.error(err);
            }
        })()
        .then(function(){
            console.log("[Line] in");
            var thisFulfillmentMessages = [];
            var thisImageObject = {
                payload:{
                    line:{
                        "type":"image",
                        "originalContentUrl": urlStock,
                        "previewImageUrl": urlStock
                    }
                }
            };
            thisFulfillmentMessages.push(thisImageObject);
    
            // var thisStickerObject = {
            //     payload:{
            //         line:{
            //             "type": "sticker",
            //             "packageId": "11537",
            //             "stickerId": "52002734"
            //         }
            //     }
            // };
            // thisFulfillmentMessages.push(thisStickerObject);
            res.json({fulfillmentMessages: thisFulfillmentMessages});
        }); 

    }else{
        res.json({
            fulfillmentText:"嗨我是GoldRush機器人，想查詢哪支股票"
        })
    }
});

function sendCards(body, res){
    console.log('[sendCards] In');
    var thisFulfillmentMessages = [];

    var thisCarouselObject = {
        payload:{
            line:{
                "type": "template",
                "altText": "this is a carousel template",
                "template": {
                    "type": "carousel",
                    "columns": [
                        {
                            "thumbnailImageUrl": "https://i.ibb.co/Zz5ry7X/buzz.jpg",
                            "imageBackgroundColor": "#FFFFFF",
                            "title": "this is menu",
                            "text": "description",
                            "defaultAction": {
                                "type": "uri",
                                "label": "View detail",
                                "uri": "https://i.ibb.co/Zz5ry7X/buzz.jpg"
                            },
                            "actions": [
                                {
                                    "type": "postback",
                                    "label": "Buy",
                                    "data": "action=buy&itemid=111"
                                },
                                {
                                    "type": "postback",
                                    "label": "Add to cart",
                                    "data": "action=add&itemid=111"
                                },
                                {
                                    "type": "uri",
                                    "label": "View detail",
                                    "uri": "https://i.ibb.co/Zz5ry7X/buzz.jpg"
                                }
                            ]
                        },
                        {
                            "thumbnailImageUrl": "https://i.ibb.co/Zz5ry7X/buzz.jpg",
                            "imageBackgroundColor": "#000000",
                            "title": "this is menu",
                            "text": "description",
                            "defaultAction": {
                                "type": "uri",
                                "label": "View detail",
                                "uri": "https://i.ibb.co/Zz5ry7X/buzz.jpg"
                            },
                            "actions": [
                                {
                                    "type": "postback",
                                    "label": "Buy",
                                    "data": "action=buy&itemid=222"
                                },
                                {
                                    "type": "postback",
                                    "label": "Add to cart",
                                    "data": "action=add&itemid=222"
                                },
                                {
                                    "type": "uri",
                                    "label": "View detail",
                                    "uri": "https://i.ibb.co/Zz5ry7X/buzz.jpg"
                                }
                            ]
                        }
                    ],
                    "imageAspectRatio": "rectangle",
                    "imageSize": "cover"
                }
            }
        }
    };
    thisFulfillmentMessages.push(thisCarouselObject);

    var thisLocationObject = {
        payload:{
            line:{
                "type": "location",
                "title": "原價屋",
                "address": "40360台中市西區英才路474號2樓", 
                "latitude": 24.152133366975082,
                "longitude": 120.66531067524058
            }
        }
    };
    thisFulfillmentMessages.push(thisLocationObject);


    var thisStickerObject = {
        payload:{
            line:{
                "type": "sticker",
                "packageId": "11537",
                "stickerId": "52002734"
            }
        }
    };

    thisFulfillmentMessages.push(thisStickerObject);


    var thisLineObject = {
        payload:{
            line:{
                type:"template",
                altText: "this is a carousel template",
                template:{
                    type:"carousel",
                    columns:[]
                }
            }
        }
    };

   for(var x=0;x<body.length;x++){
    var thisObject = {};
    thisObject.thumbnailImageUrl = body[x].Photo;
    thisObject.imageBackgroundColor = "#FFFFFF";
    thisObject.title = body[x].Name;
    thisObject.text = body[x].Category;
    thisObject.defaultAction = {};
    thisObject.defaultAction.type = "uri";
    thisObject.defaultAction.label = "view detail";
    thisObject.defaultAction.uri = body[x].Photo;
    thisObject.actions = [];
    var thisActionObject = {};
    thisActionObject.type = "uri";
    thisActionObject.label = "view detail";
    thisActionObject.uri = body[x].Photo;
    thisObject.actions.push(thisActionObject);
    thisLineObject.payload.line.template.columns.push(thisObject);
    }

    thisFulfillmentMessages.push(thisLineObject);
    var responseObject = {
        fulfillmentMessages:thisFulfillmentMessages
    };
    res.json(responseObject);
}