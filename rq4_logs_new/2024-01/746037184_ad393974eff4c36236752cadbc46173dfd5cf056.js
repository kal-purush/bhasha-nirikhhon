// Initialize the Leaflet map 
var map = L.map('map').setView([37.7, -122.4], 10);

// Add a tile layer 
L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}', {
    attribution: 'Esri, HERE, Garmin, © OpenStreetMap',
    maxZoom: 16
}).addTo(map);

// Define a custom icon for markers
var ratIcon = L.icon({
    iconUrl: 'http://maptimeboston.github.io/leaflet-intro/rat.gif',
    iconSize: [50, 40]
});

// Load GeoJSON data from a URL and add it to the map
$.getJSON("https://raw.githubusercontent.com/orhuna/WebGIS_SLU_M1/main/Module%201/Assignment%201/data/sf_crime.geojson", function(data) {
    L.geoJson(data, {
        pointToLayer: function(feature, latlng) {
            var marker = L.marker(latlng, {icon: ratIcon});

            // Bind a popup to the marker if there is a description in the feature properties
            if (feature.properties && feature.properties.description) {
                marker.bindPopup(feature.properties.description);
            }
            return marker;
        }
    }).addTo(map);
});