// USGS 7 Day Earth Quake geoJson data
var url =
  "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson";

// geoJson data to be used by the map
var geoJson = null;

// Color pallette to use for circles
var colors = [
  "#008000",
  "#ADFF2F",
  "#FFFF00",
  "#FFAE42",
  "#FFA500",
  "#FF4500",
  "#FF0000",
];

//
// Get a color based upon the range of values
// use that to display the circles and legend
//
function getColors(value) {
  var valueColor = colors[0];
  switch (true) {
    case value < 10.0:
      valueColor = colors[0];
      break;
    case value < 30.0:
      valueColor = colors[1];
      break;
    case value < 50.0:
      valueColor = colors[2];
      break;
    case value < 70.0:
      valueColor = colors[3];
      break;
    case value < 90.0:
      valueColor = colors[4];
      break;
    case value < 110.0:
      valueColor = colors[5];
      break;
    default:
      valueColor = colors[6];
  }
  return valueColor;
}

//
// Fetch the data and pass the initial function you want called after.
// This also sets the global geoJson variable to be used later in the
// code
//
function initData(initFunc) {
  d3.json(url)
    .then(
      function (data) {
        // Store the data into the global variable
        geoJson = data;
        // Convert the datatype so no issues
        geoJson.features.forEach(function (feature) {
          feature.geometry.coordinates[2] = +feature.geometry.coordinates[2];
          feature.properties.mag = +feature.properties.mag;
        });
      },
      function (error) {
        console.log(error);
      }
    )
    .then(initFunc);
}

//
// Get a circle radius base on magnatude
//
function getRadius(magnitude) {
  return magnitude * 3;
}

//
// Give each feature a popup describing the place, magnitude, depth and
// time of the earthquake
//
function onEachFeatureFunc(feature, layer) {
  layer.bindPopup(
    "<h3>" +
      feature.properties.place +
      "</h3><hr>" +
      "Magnitude: " +
      feature.properties.mag +
      "<br/>" +
      "Depth: " +
      feature.geometry.coordinates[2] +
      "<br/>" +
      new Date(feature.properties.time)
  );
}

//
// Need to set the properties of the circle marker for the earthquake
//
function pointToLayerFunc(feature, latlng) {
  var color = getColors(feature.geometry.coordinates[2]);

  // Return the marker at the lat-long
  return L.circleMarker(latlng, {
    radius: getRadius(feature.properties.mag),
    fillColor: color,
    color: color,
    weight: 1,
    opacity: 1,
    fillOpacity: 0.8,
  });
}

//
// Initialize the mapping visualization
//
function init() {
  // Load the earthquake data via geoJSON
  var earthquakes = L.geoJSON(geoJson.features, {
    onEachFeature: onEachFeatureFunc,
    pointToLayer: pointToLayerFunc,
  });

  // Create base layers
  // Satellite Layer
  var satelliteMap = L.tileLayer(
    "https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}",
    {
      attribution:
        "© <a href='https://www.mapbox.com/about/maps/'>Mapbox</a> © <a href='http://www.openstreetmap.org/copyright'>OpenStreetMap</a> <strong><a href='https://www.mapbox.com/map-feedback/' target='_blank'>Improve this map</a></strong>",
      tileSize: 512,
      maxZoom: 18,
      zoomOffset: -1,
      id: "mapbox/satellite-v9",
      accessToken: API_KEY,
    }
  );

  // Grayscale Layer
  var grayscaleMap = L.tileLayer(
    "https://api.mapbox.com/styles/v1/mapbox/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}",
    {
      attribution:
        'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
      maxZoom: 18,
      id: "light-v10",
      accessToken: API_KEY,
    }
  );

  // Outdoors Layer
  var outdoorsMap = L.tileLayer(
    "https://api.mapbox.com/styles/v1/mapbox/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}",
    {
      attribution:
        'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
      maxZoom: 18,
      id: "outdoors-v11",
      accessToken: API_KEY,
    }
  );

  // Define a baseMaps object to hold our base layers
  var baseMaps = {
    Satellite: satelliteMap,
    Grayscale: grayscaleMap,
    Outdoors: outdoorsMap,
  };

  // Create overlay object to hold our overlay layer
  var overlayMaps = {
    Earthquakes: earthquakes,
  };

  // Create our map, giving it the streetmap and earthquakes layers to display on load
  var myMap = L.map("mapid", {
    center: [28.0, -35.0],
    zoom: 3,
    layers: [satelliteMap, earthquakes],
  });

  // Place the legend control
  var legend = L.control({ position: "bottomright" });

  // Build the legend based upon the color scale ranges and label
  legend.onAdd = function (myMap) {
    var div = L.DomUtil.create("div", "info legend");
    var limits = [10, 30, 50, 70, 90, 110, 110];
    var labels = [];

    // Structure you legend html
    var myHtmlBegin =
      "<div class='my-legend'>" +
      "  <div class='legend-title'>Earthquake Depth - Past 7 Days</div>" +
      "     <div class='legend-scale'>" +
      "      <ul class='legend-labels'>";
    var myHtmlEnd =
      "      </ul>" +
      "    </div>" +
      "  <div class='legend-source'>Source: <a href='https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php' target='_blank'>USGS Earthquake Hazards Program</a></div>" +
      "</div>";

    // Loop through setting up the legend colors and labels
    var prevValue = 0;
    limits.forEach(function (limit, index) {
      // default to the standard label
      var textLabel = prevValue + " - " + limits[index];
      // First and last element require special formatting
      if (prevValue === 0) {
        textLabel = "< " + limits[index];
      }
      if (index === limits.length - 1) {
        textLabel = "> " + limits[index];
      }

      // Add this label to the array
      labels.push(
        '<li><span style="background:' +
          colors[index] +
          '";></span>' +
          textLabel +
          "</li>"
      );

      // Update previous values
      prevValue = limits[index];
    });

    // construct the HTML for the legend
    div.innerHTML = myHtmlBegin + labels.join("") + myHtmlEnd;

    return div;
  };

  // Add the legend to the map
  legend.addTo(myMap);

  // Create a layer control
  // Pass in our baseMaps and overlayMaps
  // Add the layer control to the map
  L.control
    .layers(baseMaps, overlayMaps, {
      collapsed: false,
    })
    .addTo(myMap);

  // When someone toggles off/on the earthquake data remove/add legend
  myMap
    .on("overlayadd", function (eventLayer) {
      // Toggle earthquake legend...
      if (eventLayer.name === "Earthquakes") {
        legend.addTo(this);
      }
    })
    .on("overlayremove", function (eventLayer) {
      // Toggle earthquake legend...
      if (eventLayer.name === "Earthquakes") {
        this.removeControl(legend);
      }
    });
}

// Execute the program
initData(init);