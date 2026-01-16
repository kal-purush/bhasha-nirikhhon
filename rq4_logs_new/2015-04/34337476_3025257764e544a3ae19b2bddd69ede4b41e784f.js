chrome.browserAction.onClicked.addListener(function(){  
     console.log("we aa");
});

var rect ={};

chrome.runtime.onMessage.addListener(function(request, sender, sendRequest){
	rect = request;
	console.log(rect)
});

function backgroundFunction () {

	// body...
	return rect.top;
}