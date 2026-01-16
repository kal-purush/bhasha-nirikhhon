//Create tilelayer for map
let streetmap = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
});

//Create map 
let myMap = L.map("map", {
  center: [36.78, -119.42],
  zoom: 13
});

// Add tilelayer to map
streetmap.addTo(myMap);

// URL to fetch the earthquake geoJSON data
let url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson";

// Fetch and process the earthquake data
d3.json(url).then(function(data) {
    console.log(data.features);
  function styling(feature) {
    return {
      radius: Math.max(1, feature.properties.mag) * 5,
      fillColor: getColor(feature.geometry.coordinates[2]),
      color: "#000000", // Border color
      weight: 1, // Border weight
      opacity: 1, // Border opacity
      fillOpacity: 0.8 // Fill opacity
    };
  }

  //Set the color for markers as per the depth of earthquake
  function getColor(depth) {
        return depth > 90 ? "#ea2c2c" :
               depth > 70 ? "#ea822c" :
               depth > 50 ? "#ee9c00" :
               depth > 30 ? "#eecc00" :
               depth > 10 ? "#d4ee00" : "#98ee00";
      }

  // Create a GeoJSON layer for the earthquake data
  L.geoJson(data, {
    pointToLayer: function(feature, latlng) {
      return L.circleMarker(latlng);
    },
    style: styling,
    onEachFeature: function(feature, layer) {
      layer.bindPopup(`Magnitude: ${feature.properties.mag}<br>Depth: ${feature.geometry.coordinates[2]}<br>Location: ${feature.properties.place}`);
    }
  }).addTo(myMap);

  // Add a legend to the map
  let legend = L.control({
    position: "bottomright"
  });

  legend.onAdd = function() {
    let div = L.DomUtil.create("div", "info legend");
    let depths = [-10, 10, 30, 50, 70, 90];
    let colors = [
      "#98ee00",
      "#d4ee00",
      "#eecc00",
      "#ee9c00",
      "#ea822c",
      "#ea2c2c"
    ];

    for (let i = 0; i < depths.length; i++) {
      div.innerHTML += `<i style='background: ${colors[i]}'></i> ${depths[i]}${depths[i + 1] ? "&ndash;" + depths[i + 1] + "<br>" : "+"}`;
    }
    return div;
  };

  legend.addTo(myMap);
});


















