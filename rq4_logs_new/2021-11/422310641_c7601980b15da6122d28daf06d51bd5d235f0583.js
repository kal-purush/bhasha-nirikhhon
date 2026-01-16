'use strict';

var gKeywords;
var gKeywordsMap;

function createKeywords(imgs = getImgs()) {
    const keywords = [];
    gKeywordsMap = {};
    imgs.forEach(img => {
        img.keywords.forEach(keyword => {
            if (!keywords.includes(keyword)) keywords.push(keyword);
            gKeywordsMap[keyword] = (!gKeywordsMap[keyword]) ? 1 : gKeywordsMap[keyword] + 1;
        })
    });
    gKeywords = keywords;
}

function getKeywords() {
    return gKeywords;
}

function getKeywordsMap() {
    return gKeywordsMap;
}