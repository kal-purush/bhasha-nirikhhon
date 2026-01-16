// Tested for 16.2 
// Call this script by placing the below line in bottom branding
// <script id="custom-eds" src="../redirect.js"></script>

/*
*Check to see if jQuery is loaded
*Stop checking after jQuery is loaded and run Customisation code
*/
var trackCall = setInterval(function () {
    if (window.jQuery) {
        clearInterval(trackCall);
        CustomFTF();
    }
}
, 10);

/*
*Function to load FTF view code
*/
function CustomFTF() {
	if ($("body").hasClass("full-text-frame")) {
		var frame_src = $("iframe")[0].src;
		//alert("redirecting to "+frame_src);
		window.location.replace(frame_src);
	}
}