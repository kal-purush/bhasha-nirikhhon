// Creating map object
var myMap = L.map("map", {
	center: [30, -10],
	zoom: 3
});

let country = [];
let lats = [];
let longs = [];
let country_uri = [];

// Adding tile layer to the map
L.tileLayer(MAP_URL, {
	attribution: "Map data &copy; <a href=\"https://www.openstreetmap.org/\">OpenStreetMap</a> contributors, <a href=\"https://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, Imagery © <a href=\"https://www.mapbox.com/\">Mapbox</a>",
	maxZoom: 18,
	minZoom: 2,
	id: 'mapbox/streets-v11',
	accessToken: API_KEY
}).addTo(myMap);

d3.csv("static/js/LatLong.txt", function(data) {
	console.log(data);
    for(var i = 0; i < data.length; i++){
    	country.push(data[i].Country);
    	lats.push(data[i].Latitude);
		longs.push(data[i].Longitude);
		country_uri.push(data[i].Playlist);
	}
	
	var myFeatureGroup = L.featureGroup().addTo(myMap).on("click", groupClick);
	var marker, playlist, latlng;

    for(var j = 0; j < lats.length; j++){
		latlng = L.latLng(lats[j], longs[j]);
		playlist = country_uri[j];
		marker = L.marker(latlng).addTo(myFeatureGroup)
		.bindPopup("<a href = #section2 >" + country[j] + "</a>");
		marker.playlist = playlist;
	};

	function groupClick(event) {
		var uri = event.layer.playlist
		console.log(uri);
		let mydata = JSON.parse(data.json);
		console.log(mydata);
	}
});

