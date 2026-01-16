// ==UserScript==
// @name         Blikudo XPath Batch Value Changer with Alert
// @namespace    http://lemurbrowser.com/
// @version      1.1
// @description  Change multiple fields by XPath and show an alert when done
// @match        https://blikudo.al/*
// @grant        none
// ==/UserScript==

(function() {
    'use strict';

    // Helper to find an element by XPath
    function getElementByXPath(xpath) {
        return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
    }

    // Array of elements and values
    var elementsToChange = [
       { xpath: '/html/body/main/div[7]/div/article[1]/div/h4', value: '111123 mali' }
    ];

    // Iterate over each item
    elementsToChange.forEach(function(item) {
        var element = getElementByXPath(item.xpath);
        if (element) {
            if (element.tagName.toLowerCase() === 'input' || element.tagName.toLowerCase() === 'textarea') {
                element.value = item.value;
            } else {
                element.innerText = item.value;
            }
        }
    });

    // Show alert after all fields are filled
    alert('✅ Scripti u exe me sukses!');

})();