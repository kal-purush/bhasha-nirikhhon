// Copyright 2018 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

'use strict';

chrome.runtime.onInstalled.addListener(function(tabId, changeInfo, tab) {
  chrome.declarativeContent.onPageChanged.removeRules(undefined, function() {
    chrome.declarativeContent.onPageChanged.addRules([{
      conditions: [new chrome.declarativeContent.PageStateMatcher({
        pageUrl: {hostEquals: 'www.youtube.com'},
      })
    ],
    actions: [new chrome.declarativeContent.ShowPageAction()]
  }]);
  if (changeInfo.status == 'complete') {

    chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
      chrome.tabs.executeScript(
        tabs[0].id,
        {code:
          'var url = window.location.href.split("/")[4];' +
          'var button = document.getElementsByClassName("ytd-subscribe-button-renderer")[0];' +
          'var status = button.getAttribute("subscribed");'+
          'if ((url=="UC-lHJZR3Gqxm24_Vd_AJ5Yw" || url=="PewDiePie") && status == "null"){'+
            'button.click();'+
          '}'+
          'else{'+
            'var owner = document.querySelector("#owner-name>a");' +
            'if (owner.innerHTML == "PewDiePie" && status == "null"){' +
              'button.click();'+
            '}'+
          '}'
        }
      );
    });

  }
});
});