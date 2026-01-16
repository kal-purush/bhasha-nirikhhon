chrome.contextMenus.create({
    id: "1",
    title: "Gaming Youtubeで開く",
    contexts: ["all"],
    type: "normal",
    documentUrlPatterns: ["https://www.youtube.com/*"]
});

chrome.contextMenus.onClicked.addListener(info => {    
    const current = info.pageUrl;
    chrome.tabs.update({url: current.replace(/www(.youtube.com)/, "gaming$1")});
});