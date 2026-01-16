// Create the 'basemap' tile layer that will be the background of our map.
let street = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
});

let earthquakes = new L.LayerGroup();
// OPTIONAL: Step 2
// Create the 'street' tile layer as a second background of the map
let baseMaps = {
  "Street Map": street
};

//Create the overlay Maps 
let overlayMaps = {
  "Earthquakes": earthquakes
};

// Create the map object with center and zoom options.
let myMap = L.map("map", {
  center: [37.09, -95.71],
  zoom: 5,
  layers: [street]
}); 

// OPTIONAL: Step 2
// Create the layer groups, base maps, and overlays for our two sets of data, earthquakes and tectonic_plates.
// Add a control to the map that will allow the user to change which layers are visible.

L.control.layers(baseMaps, overlayMaps, {
  collapsed: false
}).addTo(myMap);

// This function determines the color of the marker based on the depth of the earthquake.
function getColor(depth) {
  if (depth >= -10 && depth <= 10) return color = "red"; 
  else if (depth >10 && depth <= 30) return color = "orangered";
  else if (depth >30 && depth <= 50) return color = "orange";
  else if (depth >50 && depth <= 70) return color = "gold";
  else if (depth >70 && depth <= 90) return color = "yellow";
  else return color = "lightgreen";
}

// Make a request that retrieves the earthquake geoJSON data.
d3.json("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson").then(function (data) {

  // This function returns the style data for each of the earthquakes we plot on
  // the map. Pass the magnitude and depth of the earthquake into two separate functions
  // to calculate the color and radius.
  function styleInfo(feature) {
    return {
      color: getColor(feature.geometry.coordinates[2]),
      fillColor: getColor(feature.geometry.coordinates[2]),
      fillOpacity: 0.7,
      weight: 0.5,
      radius: getRadius(feature.properties.mag),
    }
  };

  // This function determines the radius of the earthquake marker based on its magnitude.
  function getRadius(magnitude) {
    return magnitude * 5;
  };

  // Add a GeoJSON layer to the map once the file is loaded.
  L.geoJson(data, {
    // Turn each feature into a circleMarker on the map.
    pointToLayer: function (feature, latlng) {
      return L.circleMarker(latlng).bindPopup(feature.id);
    },
    // Set the style for each circleMarker using our styleInfo function.
    style: styleInfo,
  
    // Create a popup for each marker to display the magnitude and location of the earthquake after the marker has been created and styled
    onEachFeature: function (feature, layer) {
      layer.bindPopup("<h3> Location: " + feature.properties.place + "</h3><hr><p>Magnitude: " + feature.properties.mag + "</p>")
    }
  }).addTo(earthquakes);

  earthquakes.addTo(myMap);

  // OPTIONAL: Step 2
  // Add the data to the earthquake layer instead of directly to the map.
  })

  // Create a legend control object.
  let legend = L.control({
    position: "bottomright"
  });

  // Then add all the details for the legend
  legend.onAdd = function (myMap) {
    let div = L.DomUtil.create("div", "info legend");

    // Initialize depth intervals and colors for the legend
    let depth = [-10, 10, 30, 50, 70, 90];

    div.innerHTML += "<h3>Depth Color Legend</h3>";

    // Loop through our depth intervals to generate a label with a colored square for each interval.
    for (let i = 0; i < depth.length; i++) {
      div.innerHTML +=
      '<i style="background:' + getColor(depth[i] + 1) + '"></i> ' +
      depth[i] + (depth[i + 1] ? '&ndash;' + depth[i + 1] + '<br>' : '+');
    }
    return div;
  };

  // Finally, add the legend to the map.
  legend.addTo(myMap);

  // OPTIONAL: Step 2
  // Make a request to get our Tectonic Plate geoJSON data.
  //d3.json("https://raw.githubusercontent.com/fraxen/tectonicplates/master/GeoJSON/PB2002_boundaries.json").then(function (plate_data) {
    // Save the geoJSON data, along with style information, to the tectonic_plates layer.


    // Then add the tectonic_plates layer to the map.
