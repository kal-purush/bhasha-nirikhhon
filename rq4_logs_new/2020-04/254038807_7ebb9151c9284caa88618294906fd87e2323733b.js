function sendToClipboard(url, title, tab) {
	// NOTE: To see console output open the background page from the extension page in developer mode.
	var content = title + '\n' + url + '\n\n' ;
	console.log('Content:');
	console.log(content);
	var textarea = document.getElementById('ta');
	textarea.value = content;
	textarea.select();
	if (document.execCommand('copy')) {
		console.log('Sent content to clipboard.');
	} else {
		console.error('Failed to send content to clipboard.');
	}
	textarea.value = '';
}

chrome.browserAction.onClicked.addListener(function(tab) {
	sendToClipboard(tab.url, tab.title, tab);
});