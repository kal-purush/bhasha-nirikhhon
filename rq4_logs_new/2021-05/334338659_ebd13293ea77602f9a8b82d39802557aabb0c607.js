const absolutePath = 'https://tenor.cards/';
const squareViewPath = 'https://tenor.cards/views/skew.html';

/**
 * ASCII to Unicode (decode Base64 to original data)
 * @param {string} b64
 * @return {string}
 */
function atou(b64) {
	return decodeURIComponent(atob(b64));
}

/**
 * Unicode to ASCII (encode data to Base64)
 * @param {string} data
 * @return {string}
 */
function utoa(data) {
	return btoa(encodeURIComponent(data));
}

/**
 * Process the input parameter. i.e. Generate text from base64 to ASCII and display.
 */
function processPathParams(isCustomPage = false) {
	var urlParams = new URLSearchParams(location.search);

	if (urlParams.has('p'))
	{
		if (!isCustomPage)
		{
			document.getElementById('dispMsg').style.display = "block";
			document.getElementById('createCardOption').style.display = "block";
			document.getElementById('inputMsg').style.display = "none";
		}
		
		let decryptedDataParam = atou(urlParams.get('p')); // base64 decode
		let messageTextElement = document.getElementById('MessageText');
		messageTextElement.innerHTML = decryptedDataParam;
	}
	if (urlParams.has('bg'))
	{
		bgColor = urlParams.get('bg')
		document.body.style.backgroundColor = bgColor;
	}
}

/**
 * Generate the processed link for the input text converted to Base64 
 */
function generateProcessedLink() {
	let text = document.getElementById("textInput").value;

	document.getElementById('urlAccess').style.display = "none";

	if(text != undefined && text.length != 0) {
		let encodedString = utoa(text).replaceAll("[^\\p{L}\\p{N}\\p{P}\\p{Z}]", "");
		let url = absolutePath + "?p=" + encodedString;
		
		setTimeout(() => {
			document.getElementById('browsableLink').value = url;
			document.getElementById('urlAccess').style.display = "block"; 
		}, 300);
	}
}

/**
 * Utitlity method to select and copy text from box to system clipboard
 */
function copyToClipboard() {
	var copyText = document.getElementById("browsableLink");

	copyText.select();
	copyText.setSelectionRange(0, 99999); // For mobile devices

	document.execCommand("copy");
}

/**
 * Count and display input text as the use inputs data on the fly
 */
function dispTextCount() {
	let text = document.getElementById("textInput").value;
	let textLength = text.length;
	//if (length === 140)
		// set color of 'inputCountStat' object to light red  	
		// Also, revert back to original color if not present
	document.getElementById("inputCountStat").innerHTML = textLength;
}