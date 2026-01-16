//Global variables for inspection and URLS
var globalQuake;
var globalFault;


var earthquake = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson";
var faultlines = "https://raw.githubusercontent.com/fraxen/tectonicplates/master/GeoJSON/PB2002_boundaries.json";


//On start up create the base map - Call the function
renderMap(earthquake, faultlines);


//Render function
function renderMap(earthquake, faultlines) {
    //Get the earthquake data
    console.log("in renderMap");
    d3.json(earthquake, function(quakeData){
        console.log(quakeData);
        globalQuake = quakeData;
        d3.json(faultlines, function(faultData){
            globalFault = faultData;
            console.log(faultData);
            console.log(quakeData);
            createFeatures(quakeData, faultData)
        })
    })
    //create the data to map onto map
    function createFeatures(quakeData, faultData) {
        //Create the markers and the data about the earthquake
        //use the functions to set the color and the size of the circle
        console.log("in createfeatures");
        console.log(faultData);
        console.log(quakeData);
        function onEachQuakeInst(feature, layer) {
            return new L.circleMarker([feature.geometry.coordinates[1], feature.geometry.coordinates[0]], {
                fillOpacity: 1,
                color: setColor(feature.properties.mag),
                fillColor: setColor(feature.properties.mag),
                radius: markSize(feature.properties.mag)
            });
        }
        function onEachQuake(feature, layer) {
            console.log("in EachQuake");
            layer.bindPopup("<h3>" + feature.properties.place + "</h3><hr><p>" + new Date(feature.properties.time) + "</p><hr><p>Magnitude: " + feature.properties.mag + "</p>");

        };

        function onEachFault(feature, layer) {
            L.polyline(feature.geometry.coordinates);
        };

        //geoJson of quakes
        var earthquakes = L.geoJSON(quakeData, {
            onEachFeature: onEachQuake,
            pointToLayer: onEachQuakeInst
        });

        //Create the geojason for faultlines
        var faultlines = L.geoJSON(faultData, {
            onEachFeature: onEachFault,
            style: {
                weight: 1,
                color: 'red'
            }
        });

        //Create the map
        createMap(earthquakes, faultlines);
        console.log(earthquakes);
        console.log("create Features Map");
    };


    //Create the map
    function createMap(earthquakes, faultlines){
        //Create the layers
        var outdoors = L.tileLayer("https://api.mapbox.com/styles/v1/mapbox/outdoors-v10/tiles/256/{z}/{x}/{y}?" +
        "access_token=pk.eyJ1IjoidHRvY3NuYW1mdWFrIiwiYSI6ImNqaDlmOHBubTAzMDYzZXMwcG9za25iYW8ifQ.5Glgswiy3oMRgwB6dPmSMQ");
        var satellite = L.tileLayer("https://api.mapbox.com/styles/v1/mapbox/satellite-v9/tiles/256/{z}/{x}/{y}?" +
        "access_token=pk.eyJ1IjoidHRvY3NuYW1mdWFrIiwiYSI6ImNqaDlmOHBubTAzMDYzZXMwcG9za25iYW8ifQ.5Glgswiy3oMRgwB6dPmSMQ");
        var darkmap = L.tileLayer("https://api.mapbox.com/styles/v1/mapbox/dark-v9/tiles/256/{z}/{x}/{y}?" +
        "access_token=pk.eyJ1IjoidHRvY3NuYW1mdWFrIiwiYSI6ImNqaDlmOHBubTAzMDYzZXMwcG9za25iYW8ifQ.5Glgswiy3oMRgwB6dPmSMQ");

        //Create the basemap
        var baseMaps = {
            "Outdoors": outdoors,
            "Satellite": satellite,
            "Dark Map": darkmap
        };

        //Create quakes and faultlines from the var passed into the function
        var mapOverlay = {
            "Earthquakes": earthquakes,
            "Faultlines": faultlines
        };

        //create map center on Omaha... The center of the US
        var map = L.map("map", {
            center: [41.2565, -95.9345],
            zoom: 4,
            layers: [outdoors, faultlines, earthquakes],
            scrollWheelZoom: false
        });

        //Layer control
        L.control.layers(baseMaps, mapOverlay, {
            collapsed: true
        }).addTo(map);

        //Legend
        var legend = L.control({position: 'bottomright'});
        legend.onAdd = function(map){
            var div = L.DomUtil.create('div', 'info legend'),
                grades = [0,1,2,3,4,5],
                labels = ['0-1', '1-2', '2-3', '3-4', '4-5', '>5'];
            //create the html tags for the grades and labels.
            for (var i = 0; i < grades.length; i++){
                div.innerHTML += '<i style="background:' + setColor(grades[i] + 1) +'"></i> ' +
                grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br> ' : '+');
            };
            return div;
        }
        legend.addTo(map);
        console.log("finished printing the map");
    };
}

// set color of circle for 
function setColor(magnitude) {
    return magnitude > 5 ? "red":
           magnitude > 4 ? "orange":
           magnitude > 3 ? "pink":
           magnitude > 2 ? "yellow":
           magnitude > 1 ? "lightgreen":
                           "lightyellow";
  };

  // marker size

function markSize(magnitude){
    return magnitude * 4;
};