var script = document.createElement("script");
script.src = chrome.extension.getURL("scripts/script.js"), script.onload = function () {
    this.parentNode.removeChild(this)
}, document.head.appendChild(script);