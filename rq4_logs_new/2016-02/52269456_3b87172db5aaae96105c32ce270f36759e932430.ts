var chrome: any;
(() => {
  var alert = new Date();
  alert.setHours(19);
  alert.setMinutes(50);

  function show() {
    chrome.browserAction.setIcon({ path: "icon1.png" });
    chrome.browserAction.setBadgeText({ text: 'open' });
    chrome.browserAction.setBadgeBackgroundColor({ color: [200, 0, 0, 255] });
  }

  function hide() {
    chrome.browserAction.setIcon({ path: "icon2.png" });
    chrome.browserAction.clearBadge();
  }

  function toggle() {
    if (alert <= new Date()) {
      show();
    } else {
      hide();
    }
  }

  chrome.alarms.create('fetch', { periodInMinutes: 5 });
  chrome.alarms.onAlarm.addListener(function(alarm) {
    if (alarm.name === 'fetch') {
      toggle();
    }
  });
  toggle();
})();