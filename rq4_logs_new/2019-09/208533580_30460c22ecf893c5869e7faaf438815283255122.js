
chrome.browserAction.onClicked.addListener(function (tab) { 
  chrome.tabs.executeScript(tab.id, {
      "file": "content.js"
  }, function () { // 
      console.log("Executed.")
  });
});