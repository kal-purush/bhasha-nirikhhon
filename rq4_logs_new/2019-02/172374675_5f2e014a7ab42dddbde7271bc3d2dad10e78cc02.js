// ==UserScript==
// @name         Slickdeals Expired Deals Auto-Hide
// @namespace    http://tampermonkey.net/
// @version      0.1
// @description  Authohides Slickdeals.net's search page's expired deals.
// @author       Stephen Chan
// @match        https://slickdeals.net/newsearch.php?*
// @grant        none
// ==/UserScript==

(function() {
    'use strict';

    let hideExpiredCheckbox = document.querySelector("input#hideExpired");

    if (hideExpiredCheckbox.checked == false) {
        // Click is used instead of setting checked = true in order to trigger the automatic page redirect
        hideExpiredCheckbox.click();
    }
})();