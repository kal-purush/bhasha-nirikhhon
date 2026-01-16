var map, windowSize, mapMaxZoom;

map = L.map("map", {
  center: [46.41, -93.19],
  zoom: 6,
  minZoom: 2,
  maxZoom: 19
});

windowSize = $(window).width();
mapMaxZoom = map.getMaxZoom();

if (windowSize > 600) {
  collapseLegend = false;
} else {
  collapseLegend = true;
}

var esriServiceUrl = 'http://server.arcgisonline.com/ArcGIS/rest/services/';
var esriAttribution = 'Tiles &copy; Esri';

var esriTopo = L.tileLayer(esriServiceUrl + 'World_Topo_Map/MapServer/tile/{z}/{y}/{x}', {
	minZoom: 2,
  maxZoom: 15,
  attribution: esriAttribution
}).addTo(map);

var esriSatellite = L.tileLayer(esriServiceUrl + 'World_Imagery/MapServer/tile/{z}/{y}/{x}', {
   minZoom: 15,
   maxZoom: mapMaxZoom,
   attribution: esriAttribution
}).addTo(map);

var esriRoadsReference = L.tileLayer(esriServiceUrl + 'Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}', {
  minZoom: 15,
  maxZoom: mapMaxZoom,
  attribution: esriAttribution
}).addTo(map);

var esriReference = L.tileLayer(esriServiceUrl + 'Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}', {
  minZoom: 15,
  maxZoom: mapMaxZoom,
  attribution: esriAttribution
}).addTo(map);

function setPopupContent(feature, layer) {
  if (feature.properties.teamName) {
    var popupContent = "<b style='font-size:20px;'>" + feature.properties.ballparkName + "</b><br/>";
        popupContent += feature.properties.teamName + "<br/>";
  } else {
    var popupContent = "<b style='font-size:20px;'>" + feature.properties.name + "</b><br/>";
  }
  if (feature.properties.comments) {
    popupContent += feature.properties.comments + "<br/><br/>";
  }
  if (feature.properties.visited) {
    popupContent += "<b>Last visited:</b> " + feature.properties.visited + "<br/>";
  }
  if (feature.properties.camping) {
    popupContent += "<b>Camping:</b> " + feature.properties.camping + "<br/>";
  }
  if (feature.properties.activities) {
    popupContent += "<b>Activities:</b> " + feature.properties.activities + "<br/>";
  }
	layer.bindPopup(popupContent);
}

var mnParks = L.geoJson(null, {
  pointToLayer: function (feature, latlng) {
    return L.circleMarker(latlng, {
      radius: 5,
      fillColor: setColor(feature.properties.visited),
      color: '#FFF',
      fillOpacity: 0.8
    });
  },
  onEachFeature: setPopupContent
});
$.getJSON("places/parks.json", function (data) {
	mnParks.addData(data).addTo(map);
});

var mlbBallparks = L.geoJson(null, {
  pointToLayer: function (feature, latlng) {
    return L.circleMarker(latlng, {
      radius: 5,
      fillColor: setColor(feature.properties.visited),
      color: '#FFF',
      fillOpacity: 0.8
    });
  },
  onEachFeature: setPopupContent
});
$.getJSON("places/ballparks.json", function (data) {
	mlbBallparks.addData(data);
});

function setColor (value) {
  switch (value) {
    case 'N/A':     return '#8E0152';
    case 'Planned': return '#DE77AE';
    default:        return '#276419';
  }
}

var overlayMaps = {
    "Ballparks": mlbBallparks,
    "State Parks": mnParks
};

L.control.layers(null, overlayMaps, {
  collapsed: collapseLegend
}).addTo(map);

//Zoom to the respective layer if checking it on in the legend
map.on('overlayadd', function (layer) {
  if (layer.name == "State Parks") { var activeLayer = mnParks;
  } else { var activeLayer = mlbBallparks; }
  map.fitBounds(activeLayer.getBounds());
});