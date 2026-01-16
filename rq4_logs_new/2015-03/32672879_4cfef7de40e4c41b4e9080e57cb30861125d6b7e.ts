/// <reference path="typings/tsd.d.ts" />

var callback = function (details) {
    var redirectUrl = details.url.replace("://ja.wikipedia.org/", "://ja.m.wikipedia.org/");
    return {"redirectUrl": redirectUrl};
};
var filter = {
    urls: [
        "*://ja.wikipedia.org/*"
    ]
};
var opt_extraInfoSpec = ["blocking"];

chrome.webRequest.onBeforeRequest.addListener(callback, filter, opt_extraInfoSpec);