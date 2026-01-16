/// <reference path="../typings/express/express.d.ts"/>
/// <reference path="../typings/node/node.d.ts"/>
"use strict";

var express = require('express');
var superAgent = require('superagent');
var cheerio = require('cheerio');

var app = express();

app.get('/', function (req, res) {
    var url = 'https://cnodejs.org/';
    var $;
    var items:Array<{href:string;title:string;}>=[];
    superAgent
        .get(url)
        .set({
            Referer: url,
            'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0"
        })
        .end(function (err, response) {
            if (response.status === 200) {
                $ = cheerio.load(response.text);
            }
            $('#topic_list .topic_title').each(function (index, item) {
                var $item = $(item);
                items.push({
                    title: $item.attr('title'),
                    href: url + $item.attr('href')
                })
            });
            if (items) {
                res.send(items);
            }
        });
});
app.listen(3000);


