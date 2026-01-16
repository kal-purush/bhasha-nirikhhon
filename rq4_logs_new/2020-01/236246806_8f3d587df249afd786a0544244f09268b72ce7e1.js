var quakeUrl = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson"
function markerSize(mag) {
    return mag * 30000;
  }
  
  function markerColor(mag) {
    if (mag <= 1) {
        return "#ADFF2F";
    } else if (mag <= 2) {
        return "#9ACD32";
    } else if (mag <= 3) {
        return "#FFFF00";
    } else if (mag <= 4) {
        return "#ffd700";
    } else if (mag <= 5) {
        return "#FFA500";
    } else {
        return "#FF0000";
    };
  }
  
  d3.json(quakeUrl, function(data) {
    createFeatures(data.features);
  });
  
  function createFeatures(earthquakeData) {
  
    var earthquakes = L.geoJSON(earthquakeData, {

   onEachFeature : function (feature, layer) {
  
      layer.bindPopup("<h3>" + feature.properties.place +
        "</h3><hr><p>" + new Date(feature.properties.time) + "</p>" + "<p> Magnitude: " +  feature.properties.mag + "</p>")
      },     pointToLayer: function (feature, latlng) {
        return new L.circle(latlng,
          {radius: markerSize(feature.properties.mag),
          fillColor: markerColor(feature.properties.mag),
          fillOpacity: 1,
          stroke: false,
      })
    }
    });
      
    createMap(earthquakes);
  }
  
  function createMap(earthquakes) {
  
    var satelitemap = L.tileLayer("https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}", {
      attribution: "Map data &copy; <a href=\"https://www.openstreetmap.org/\">OpenStreetMap</a> contributors, <a href=\"https://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, Imagery © <a href=\"https://www.mapbox.com/\">Mapbox</a>",
      maxZoom: 18,
      id: "mapbox.satellite",
      accessToken: "pk.eyJ1IjoiYnJ5YW5vODQiLCJhIjoiY2s1ZWg5cTljMjcyaTNscDNxaG5jbWdxaSJ9.UnUbRRe-t3HU60rg4I4Y6Q"
    });
  
    var darkmap = L.tileLayer("https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}", {
      attribution: "Map data &copy; <a href=\"https://www.openstreetmap.org/\">OpenStreetMap</a> contributors, <a href=\"https://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, Imagery © <a href=\"https://www.mapbox.com/\">Mapbox</a>",
      maxZoom: 18,
      id: "mapbox.dark",
      accessToken: "pk.eyJ1IjoiYnJ5YW5vODQiLCJhIjoiY2s1ZWg5cTljMjcyaTNscDNxaG5jbWdxaSJ9.UnUbRRe-t3HU60rg4I4Y6Q"
    });
  
    var baseMaps = {
      "Satelite Map": satelitemap,
      "Dark Map": darkmap
    };
  
    var overlayMaps = {
      Earthquakes: earthquakes
    };

    var myMap = L.map("map", {
      center: [31.57853542647338,-99.580078125],
      zoom: 3,
      layers: [satelitemap, earthquakes]
    });

    L.control.layers(baseMaps, overlayMaps, {
      collapsed: false
    }).addTo(myMap);
  
    var legend = L.control({position: 'bottomright'});
  
    legend.onAdd = function () {
    
        var div = L.DomUtil.create('div', 'info legend'),
            magnitudes = [0, 1, 2, 3, 4, 5];
    
        for (var i = 0; i < magnitudes.length; i++) {
            div.innerHTML +=
                '<i style="background:' + markerColor(magnitudes[i] + 1) + '"></i> ' + 
        + magnitudes[i] + (magnitudes[i + 1] ? ' - ' + magnitudes[i + 1] + '<br>' : ' + ');
        }
    
        return div;
    };
    
    legend.addTo(myMap);
  
  }


  