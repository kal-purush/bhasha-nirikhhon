//var GOOGLE_BASE_URL = 'http://maps.google.com/maps/api/geocode/json?address=';
var ERROR_MESSAGE = 'There was an error geocoding. Please try again.';


var GOOGLE_BASE_URL = "https://maps.googleapis.com/maps/api/geocode/json?address=";
var GOOGLE_KEY = '&key=AIzaSyAl7zrQ1cB9Zw-3iMEqq6Gg3llze5tWuxk';
	
exports.LATITUDE_BASE = 37.389569;
exports.LONGITUDE_BASE = -122.050212;
var XHR = require("/xhr");
var xhr = new XHR();

var GeoData = function(title, latitude, longitude) {
	this.title = title;
	this.coords = {
		latitude: latitude,
		longitude: longitude
	};
};

exports.forwardGeocode = function(address, callback) {
	if (Ti.Platform.osname === 'mobileweb') {
		forwardGeocodeWeb(address, callback);
	} else {
		forwardGeocodeNative(address, callback);
	}
};

var forwardGeocodeNative = function(address, callback) {
	var url = GOOGLE_BASE_URL + address.replace(' ', '+');
	url += GOOGLE_KEY;
	
	var success=function(e) {
		/*
		var json = JSON.parse(this.responseText);
		if (json.status != 'OK') {
			alert('Unable to geocode the address');
			return;
		}
		*/
Ti.API.info("result "+ JSON.stringify(e.data));
		
			//Ti.API.info("result "+this.responseText);
	
	};
	
	var error = function(e) {
		Ti.API.error(e.error);
		alert(ERROR_MESSAGE);
	};
	xhr.get(url, success, error);	
	
	
};

var forwardGeocodeWeb = function(address, callback) {
	var geocoder = new google.maps.Geocoder();
	if (geocoder) {
		geocoder.geocode({
			'address': address
		}, function(results, status) {
			if (status == google.maps.GeocoderStatus.OK) {
				callback(new GeoData(
					address,
					results[0].geometry.location.lat(),
					results[0].geometry.location.lng()
				));
			} else {
				Ti.API.error(status);
				alert(ERROR_MESSAGE);
			}
		});
	} else {
		alert('Google Maps Geocoder not supported');
	}
};