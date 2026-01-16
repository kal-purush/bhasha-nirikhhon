let protectionEnabled = false;

// Initialize protection status on startup
chrome.storage.local.get("protectionStatus", (data) => {
  protectionEnabled = data.protectionStatus === "active";
});

// Listen for protection status changes
chrome.storage.onChanged.addListener((changes) => {
  if (changes.protectionStatus) {
    protectionEnabled = changes.protectionStatus.newValue === "active";
  }
});

// 🔄 Enforce fullscreen on tab update
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.status === "complete") {
    chrome.storage.local.get("protectionStatus", (data) => {
      const protectionStatus = data.protectionStatus || "inactive";

      // Skip sign-in or sign-up pages on any website
      if (tab.url.includes("/signin") || tab.url.includes("/signup")) {
        console.log("Sign-in/Sign-up page detected. No protection applied.");
        return;
      }

      // Apply fullscreen if protection is active
      if (protectionStatus === "active") {
        console.log("Protection active. Enforcing fullscreen mode.");
        chrome.scripting.executeScript({
          target: { tabId: tabId },
          func: enableFullscreen
        });
      }
    });
  }
});

// 🛡️ Block new tabs when protection is active

chrome.windows.onBoundsChanged.addListener((window) => {
  chrome.windows.get(window.id, (win) => {
    if (win.state !== "fullscreen") {
      chrome.windows.update(win.id, { state: "fullscreen" });
    }
  });
});

// Reset protection status on Chrome startup
chrome.runtime.onStartup.addListener(() => {
  chrome.storage.local.set({ protectionStatus: "inactive" }, () => {
    console.log("Protection status reset to inactive on Chrome startup.");
  });
});

// Optional: Also reset on extension install/update
chrome.runtime.onInstalled.addListener(() => {
  chrome.storage.local.set({ protectionStatus: "inactive" }, () => {
    console.log("Protection status set to inactive on install/update.");
  });
});
