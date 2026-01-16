/* DETECT */
var dspdocinfo = 0;

// DESKTOP or DEVICE /////////////////////////////////////////////////////////////////////////
// http://ru05team.blogspot.nl/2011/09/using-javascript-to-solve-mobile.html
// http://blog.justin.kelly.org.au/simple-javascript-mobile-os-detector/
var uagent = navigator.userAgent.toLowerCase();

function detectUserAgent(deviceName){
	return (uagent.search(deviceName) > -1)
}

function isDevice(){
	return (detectUserAgent("iphone") ||
			detectUserAgent("ipod") ||
			detectUserAgent("palm") ||
			detectUserAgent("android") ||
			detectUserAgent("series60") ||
			detectUserAgent("symbian") ||
			detectUserAgent("windows ce") ||
			detectUserAgent("windows phone") ||
			detectUserAgent("blackberry")
		); // ipad left out - only needs touch events
}

function isDesktop() {
	if(isDevice()){
		return false;
	}else {
		return true
	}
}

// DEVICE TYPE ///////////////////////////////////////////////////////////////////////////////
function isTouch(){
   var el = document.createElement('div');
   el.setAttribute('ongesturestart', 'return;');
   if(typeof el.ongesturestart == "function"){
	  return true;
   }else {
	  return false
   }
}

// FUNCTION SUPPORT ///////////////////////////////////////////////////////////////////////////
function fixedSupport() { // http://kangax.github.com/cft/#IS_POSITION_FIXED_SUPPORTED
	var container = document.body;
	if (document.createElement &&
		container && container.appendChild && container.removeChild) {
		var el = document.createElement("div");
		if (!el.getBoundingClientRect) {
			return null;
		}
		el.innerHTML = "x";
		el.style.cssText = "position:fixed;top:100px;";
		container.appendChild(el);
		var originalHeight = container.style.height, originalScrollTop = container.scrollTop;
		container.style.height = "3000px";
		container.scrollTop = 500;
		var elementTop = el.getBoundingClientRect().top;
		container.style.height = originalHeight;
		var isSupported = elementTop === 100;
		container.removeChild(el);
		container.scrollTop = originalScrollTop;
		return isSupported;
	}
	return null;
}
// start stop scroll events: http://james.padolsey.com/demos/scrollevents/

// DOC DETECT RESULT INFO
function dsplDocinfo(){
	var docinfo = '<h3>Document Handling Info</h3>';
	if(isDevice()){
		docinfo += '<p>Device platform</p>';
	}else{
		docinfo += '<p>Desktop platform</p>';
	}
	if(isTouch()){
		docinfo += '<p>Touch gestures</p>';
	}
	if(fixedSupport() == null){
		docinfo += '<p>No fixed position support</p>';
	}

	var message = $("<div/>", {
	id: "dynamicbox",
	html: docinfo
	}).css({
    	top: '10px',
    	left: '10px',
		padding: '10px 15px',
		background: '#CC0000',
		position: 'absolute'
    }).appendTo("body");
}

// INIT ///////////////////////////////////////////////////////////////////////////////////////

	if(dspdocinfo == 1){
		dsplDocinfo();
	}