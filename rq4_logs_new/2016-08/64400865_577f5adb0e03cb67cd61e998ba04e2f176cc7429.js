

/**
* Make request to change the items status
*/
function checkinCheckoutNew(nid,barcode,mode,view_display) {

	http_request = false;
	if (window.XMLHttpRequest) { // Mozilla, Safari,...
		http_request = new XMLHttpRequest();
		if (http_request.overrideMimeType) {
			http_request.overrideMimeType('text/xml');
				// zu dieser Zeile siehe weiter unten
		}
	} else if (window.ActiveXObject) { // IE
		try {
			http_request = new ActiveXObject("Msxml2.XMLHTTP");
			} catch (e) {
				try {
					http_request = new ActiveXObject("Microsoft.XMLHTTP");
				} catch (e) {}
			}
		}
 
	if (!http_request) {
		alert('Ende :( Kann keine XMLHTTP-Instanz erzeugen');
		return false;
	}
	http_request.onreadystatechange =response(nid,mode);
	http_request.open('GET', 'http://'+window.location.hostname+'?q=mediathek_util_ajax_response&nid='+nid+'&barcode='+barcode+'&mode='+mode+'&view_display='+view_display, true);
	http_request.send(null);
}