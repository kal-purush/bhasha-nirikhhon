// Listen for messages from content scripts
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (message.action === "storeAIStatus") {
        // Store AI detection status associated with the current tab ID
        chrome.storage.local.set({ [`tab_${sender.tab.id}`]: message.aiDetected });
    } else if (message.action === "getAIStatus") {
        // Retrieve AI detection status for the current tab
        chrome.storage.local.get(`tab_${message.tabId}`, (data) => {
            sendResponse({ aiDetected: data[`tab_${message.tabId}`] });
        });
        return true; // Keeps the message channel open for asynchronous response
    }
});