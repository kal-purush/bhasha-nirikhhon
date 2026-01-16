//
//  Douban Extension (Chrome)
//
//  Created by Kevin (haoxi911@gmail.com)
//
//  Copyright (c) 2017 Kevin (Hao)
//  All rights reserved.
//

// Root URL of rexxar API.
var rexxarAPI = 'https://m.douban.com/rexxar/api/v2/';

// Default extension settings.
var defaultSettings = {

  /* 0 (user avatar), 1 (gender icon) */
  avatar: 0,

  /* tags: settings for each tag */
  tags: [
    { name: 'newreg', limit: 7, unit: 'D' },
    { name: 'oldtopic', limit: 30, unit: 'D' }
  ],

  /* filter topics by gender: 0 (all), 1 (female), 2 (male) */
  filter_gender: 0,

  /* filter topics by tags */
  filter_tags: ['blacklist'],

  /* filter topics by contents */
  filter_contents: []
};

// This will block some specific URL requests.
chrome.webRequest.onBeforeRequest.addListener(
  function(details) {
    return {
      cancel: true
    };
  }, {
    urls: ["https://img3.doubanio.com/f/shire/a30aa182ef41ace9fcbb410b1ea7142926f36616/js/ad.js"]
  }, ["blocking"]
);

// This will make sure the 'Referer' header set to 'https://m.douban.com/group/'
// for all rexxar API calls.
// Based on some rough testings, missing 'Referer' header will cause 403 error.
chrome.webRequest.onBeforeSendHeaders.addListener(
  function(details) {

    var refererKey = 'Referer';
    var refererValue = 'https://m.douban.com/group/';
    var hasReferer = false;

    for (var i = 0; i < details.requestHeaders.length; ++i) {
      if (details.requestHeaders[i].name == refererKey) {
        details.requestHeaders[i].value = refererValue;
        hasReferer = true;
        break;
      }
    }
    if (!hasReferer) {
      details.requestHeaders.push({
        name: refererKey,
        value: refererValue
      });
    }

    return {
      requestHeaders: details.requestHeaders
    };
  }, {
    urls: ["https://m.douban.com/rexxar/api/v2/*"]
  }, ["blocking", "requestHeaders"]
);

// Handle 'requestMyTopics' message sent by 'chrome.runtime.sendMessage' method
// and compose an AJAX request to corresponding rexxar API.
// Sample request:
// {action: 'requestMyTopics', start: 100, count: 50, cookie: 'ABCD'}
function handleRequestMyTopics(request, sender, sendResponse) {

  if (request.start >= 0 && request.count > 0 && request.cookie) {
    var url = rexxarAPI + 'group/user/recent_topics?start=' +
      request.start + '&count=' + request.count + '&ck=' + request.cookie;
    $.ajax({ /* this will set cookies of 'm.douban.com' */
      url: 'https://m.douban.com/group/',
      type: "GET"
    }).then(function(){
      $.ajax({
          url: url,
          type: "GET",
          beforeSend: function(xhr) {
            xhr.setRequestHeader('Accept', 'application/json');
            xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
          }
        })
        .done(function(response) {
          sendResponse(response);
        });
    });
  }

  // Return 'true' will assume an async response, see document:
  // https://developer.chrome.com/extensions/runtime#event-onMessageExternal
  return true;
}

// Handle 'requestGroupTopics' message sent by 'chrome.runtime.sendMessage' method
// and compose an AJAX request to corresponding rexxar API.
// Sample request:
// {action: 'requestGroupTopics', group: '10126', start: 100, count: 50}
function handleRequestGroupTopics(request, sender, sendResponse) {

  if (request.group && request.start >= 0 && request.count > 0) {
    var url = rexxarAPI + 'group/' + request.group + '/topics?start=' +
      request.start + '&count=' + request.count;
    $.ajax({
        url: url,
        type: "GET",
        beforeSend: function(xhr) {
          xhr.setRequestHeader('Accept', 'application/json');
          xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
        }
      })
      .done(function(response) {
        sendResponse(response);
      });
  }

  // Return 'true' will assume an async response, see document:
  // https://developer.chrome.com/extensions/runtime#event-onMessageExternal
  return true;
}

// Handle 'requestUserInfo' message sent by 'chrome.runtime.sendMessage' method
// and compose an AJAX request to corresponding rexxar API.
// Sample request:
// {action: 'requestUserInfo', uid: '101845001'}
function handleRequestUserInfo(request, sender, sendResponse) {

  if (request.uid) {
    var url = rexxarAPI + 'user/' + request.uid;
    $.ajax({
        url: url,
        type: "GET",
        beforeSend: function(xhr) {
          xhr.setRequestHeader('Accept', 'application/json');
          xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
        }
      })
      .done(function(response) {
        sendResponse(response);
      });
  }

  // Return 'true' will assume an async response, see document:
  // https://developer.chrome.com/extensions/runtime#event-onMessageExternal
  return true;
}

// Handle 'requestImageURLs' message sent by 'chrome.runtime.sendMessage' method
// and respond an array of full access URLs to the embedded image resources.
// Sample request:
// {action: 'requestImageURL', images: ['warning.svg', 'icon.svg']}
function handleRequestImageURLs(request, sender, sendResponse) {
  var urls = [];
  for (var i = 0; i < request.images.length; i++) {
    urls.push(chrome.runtime.getURL('img/' + request.images[i]));
  }
  sendResponse({urls: urls});
}

// Handle 'requestVersion' message sent by 'chrome.runtime.sendMessage' method
// and respond the software version string.
// Sample request:
// {action: 'requestVersion'}
function handleRequestVersion(request, sender, sendResponse) {
  var manifestData = chrome.runtime.getManifest();
  sendResponse({version: manifestData.version});
}

// Handle 'requestBlacklist' message sent by 'chrome.runtime.sendMessage' method
// and respond an array of blacklisted user URLs.
// Sample request:
// {action: 'requestBlacklist', reload: false}
function handleRequestBlacklist(request, sender, sendResponse) {

  // Whether we reload blacklist from '/contacts/blacklist'.
  var needReload = request.reload || false;

  // Read blacklist from storage API.
  chrome.storage.sync.get('blacklist', function(data) {

    if (data.blacklist && data.blacklist.update_time) {
      var update_time = data.blacklist.update_time;
      var date = new Date();
      date.setDate(date.getDate() - 1);
      if (!needReload) { /* reload after each day */
        needReload = (getDateFromString(update_time) < date);
      }
    } else {
      needReload = true; /* first time */
    }

    // Read blacklist from '/contacts/blacklist'.
    if (needReload) {
      $.get('https://www.douban.com/contacts/blacklist')
       .done(function(response) {
          var urls = [];
          var $html = $(response);
          $html.find('dd > a').each(function() {
            urls.push(this.href);
          });

          // Save into extension storage.
          var blacklist = {};
          blacklist.urls = urls;
          blacklist.update_time = $.format.date(new Date(), 'yyyy-MM-dd HH:mm:ss');
          chrome.storage.sync.set({
            'blacklist': blacklist
          }, function() {
            sendResponse(blacklist);
          });
        });
    } else {
      sendResponse(data.blacklist);
    }
  });

  // Return 'true' will assume an async response, see document:
  // https://developer.chrome.com/extensions/runtime#event-onMessageExternal
  return true;
}

// Handle 'requestSettings' message sent by 'chrome.runtime.sendMessage' method
// and return the latest settings with a timestamp.
// Sample request:
// {action: 'requestSettings'}
function handleRequestSettings(request, sender, sendResponse) {

  // Read settings from storage API.
  chrome.storage.sync.get('settings', function(data) {

    // Use default settings.
    if (!data.settings) {
      var settings = defaultSettings;
      settings.update_time = $.format.date(new Date(), 'yyyy-MM-dd HH:mm:ss');
      chrome.storage.sync.set({'settings': settings}, function() {
        sendResponse(settings);
      });
    } else {
      sendResponse(data.settings);
    }
  });

  // Return 'true' will assume an async response, see document:
  // https://developer.chrome.com/extensions/runtime#event-onMessageExternal
  return true;
}

// Handle 'updateSettings' message sent by 'chrome.runtime.sendMessage' method
// and save extension settings with a timestamp using Chrome storage API.
// Sample request:
// {action: 'updateSettings', settings: {...}}
function handleUpdateSettings(request, sender, sendResponse) {

  // Set a timestamp to the updated settings.
  var settings = request.settings;
  if (settings) {
    settings.update_time = $.format.date(new Date(), 'yyyy-MM-dd HH:mm:ss');

    // Save settings.
    chrome.storage.sync.set({'settings': settings}, function() {
      sendResponse();
    });
  }

  // Return 'true' will assume an async response, see document:
  // https://developer.chrome.com/extensions/runtime#event-onMessageExternal
  return true;
}

// Handle message sent from content scripts or client webpages.
function handleRequests(request, sender, sendResponse) {
  if (request.action == 'requestMyTopics') {
    return handleRequestMyTopics(request, sender, sendResponse);
  } else if (request.action == 'requestGroupTopics') {
    return handleRequestGroupTopics(request, sender, sendResponse);
  } else if (request.action == 'requestUserInfo') {
    return handleRequestUserInfo(request, sender, sendResponse);
  } else if (request.action == 'requestImageURLs') {
    return handleRequestImageURLs(request, sender, sendResponse);
  } else if (request.action == 'requestBlacklist') {
    return handleRequestBlacklist(request, sender, sendResponse);
  } else if (request.action == 'requestSettings') {
    return handleRequestSettings(request, sender, sendResponse);
  } else if (request.action == 'updateSettings') {
    return handleUpdateSettings(request, sender, sendResponse);
  } else if (request.action == 'requestVersion') {
    return handleRequestVersion(request, sender, sendResponse);
  }
}

chrome.runtime.onMessage.addListener(handleRequests);
chrome.runtime.onMessageExternal.addListener(handleRequests);