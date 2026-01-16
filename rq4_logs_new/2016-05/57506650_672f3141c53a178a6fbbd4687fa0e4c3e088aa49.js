/* This will save all of the deleted element list. */
var removedElements = [];

/* Send request to remove element. */
var removeElement = function(e) {
	chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
	  chrome.tabs.sendMessage(tabs[0].id, {text:"removeElement"});
	});
};

/* Store removed elements. */
chrome.runtime.onMessage.addListener(function(message){
	if(typeof message.elementRemoved === 'object') {
		removedElements.push(message.removedElements);
		alert('Removed Element: ' + message.removedElements + ' - Total Elements Removed: ' + removedElements.length);
	}
});

/* Right click menu. */
chrome.contextMenus.create({
    "title": "Remove Element",
    "contexts": ["page", "selection", "image", "link"],
    "onclick" : removeElement
});