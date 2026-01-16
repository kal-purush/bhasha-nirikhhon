/// <reference path="typings/node/node.d.ts"/>
/// <reference path="typings/express/express.d.ts"/>
"use strict";

var superAgent = require('superagent');
var cheerio = require('cheerio');
var async = require('async');
// url模块是Node.js标准库里面的
var url = require('url');

var cnodeUrl = "https://cnodejs.org/";
// 每一个帖子的url
var urlList:Array<string> = [];
// 每一个用户主页的url
var userInfoUrlList:Array<string> = [];
var myResult;
var startTime = 0;
var endTime = 0;

/**
 * 获取cnodejs的主页,然后从中分析出每一个帖子的url
 * 并获取每一次帖子的基本信息(除了第一个回复用户的积分以外)
 * @param callback
 */
function getIndexPage(callback) {
    superAgent
        .get(cnodeUrl)
        .set({
            'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0"
        })
        .end(function (err, res) {
            if (err) {
                console.log(err.status);
                callback(err.status);
                return false;
            }
            if (res.status === 200) {
                var $ = cheerio.load(res.text);
                $('#topic_list .topic_title').each(function (index, item) {
                    var $item = $(item);
                    var href = url.resolve(cnodeUrl, $item.attr('href'));
                    urlList.push(href);
                });
                callback();
            }
        });
    return true;
}


/**
 * 在mapLimit中使用,获取用户的数据
 * @param item 每一个帖子的url
 * @param callback
 */
function handleUserData(item, callback) {
    superAgent
        .get(item)
        .set({
            'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0",
            'Referer': cnodeUrl
        })
        .end(function (err, res) {
            if (err) {
                console.log(item + " : " + err.status);
            }
            if (res.status === 200) {
                var $ = cheerio.load(res.text);
                var _title = $('.topic_full_title').text();
                var reply_content = $('#reply1 > .reply_content p').text();
                var user_info_url = $('#reply1  .user_avatar').attr('href');
                // 有可能没有用户回复这个帖子,所以user_info和replay_content为空
                if (user_info_url) {
                    user_info_url = url.resolve(cnodeUrl, user_info_url);
                } else {
                    user_info_url = '';
                }
                userInfoUrlList.push(user_info_url);
                callback(null, {
                    title: _title,
                    href: item,
                    commont1: reply_content,
                    userInfoUrl: user_info_url
                });
            }
        });
}

/**
 * 在mapLimit中使用,获取用户的积分
 * @param item 用户主页的url
 * @param callback
 */
function handleUserScore(item, callback) {
    // 有时候一个帖子没有用户回复,那么这个item就是空的
    if (!item) {
        callback(null, '');
        return;
    }
    superAgent
        .get(item)
        .set({
            'User-Agent': "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0",
            'Referer': cnodeUrl
        })
        .end(function (err, res) {
            if (err) {
                console.log(err);
                callback(err.status);
            }
            if (res.status === 200) {
                var $ = cheerio.load(res.text);
                var score = $('.user_card span.big').text();
                callback(null, score);
            }
        })
}


/**
 *
 * 获取用户数据,用户积分将使用getScore来完成
 * @param callback
 * @returns {boolean}
 */
function getUserData (callback) {
    startTime = Date.now();
    async.mapLimit(urlList, 10, handleUserData, function (err, result) {
        if (err) {
            console.log(err);
            callback(err.status);
            return false;
        } else {
            endTime = Date.now() - startTime;
            console.log('获取用户基本数据花费的时间: ' + endTime + '毫秒');
            myResult = result;
            callback();
            return true;
        }
    });
}

/**
 * 获取用户积分
 * @param callback
 */
function getUserScore(callback) {
    startTime = Date.now();
    async.mapLimit(userInfoUrlList, 10, handleUserScore, function (err, result) {
        if (err) {
            console.log(err);
            callback(err.status);
            return false;
        }  else {
            endTime = Date.now() - startTime;
            console.log('获取用户积分花费的时间: ' + endTime + '毫秒');
            result.forEach(function (item, index, array) {
                myResult[index]['score'] = item;
            });
            callback();
            return true;
        }
    });
}

async.waterfall([getIndexPage, getUserData, getUserScore], function (err, result) {
    if (err) {
        console.log(err);
        return false;
    } else {
        console.log(myResult);
    }
});
