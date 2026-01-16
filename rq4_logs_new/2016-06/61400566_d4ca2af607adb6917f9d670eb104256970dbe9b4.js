/**
 * @author Jawish Hameed <jawish@gmail.com>
 * @version 0.1
 * @date 20-12-2015
 *
 *
 * Expects the following config:
 * var config {
 *     linkFile = 'data.csv',
 *     weightMin = 0,
 *     weightMax = 80,
 *     lineColorMode = 'fixed', // fixed, random, weight, atoll or a colorname
 *     lineColorIn = 'red',
 *     lineColorOut = 'green',
 *     baseMapProvider = 'esri', // esri, osm
 *     startLatLng = [3.175, 73.509],
 *     startZoom = 7
 * }
 *
 */

var atollLayers = {};
var linkLayers = {};
var atollSelected = null;

// Create map and center around Maldives
var esriMap = L.tileLayer('http://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
    });
var osmMap = L.tileLayer('https://services.arcgisonline.com/ArcGIS/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
    });
var watercolor = L.tileLayer('http://c.tile.stamen.com/watercolor/{z}/{x}/{y}.jpg', { 
        attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors' 
    });

var map = L.map('map', {
    center: config.startLatLng,
    zoom: config.startZoom,
    layers: [ 
        (config.baseMapProvider == 'esri') ? esriMap : osmMap
    ]
});

// Load the atolls GeoJSON file
d3.json('atolls.geojson', function(error, atolls) {
    if (error) throw error;

    var atollLayer = L.geoJson(atolls, {
        onEachFeature: function (feature, layer) {

            var atollStyle = {
                'opacity': 0.1,
                'color': null
            };

            var immigrants = 0,
                emigrants = 0;

            layer
                .setStyle(atollStyle)
                .bindLabel(feature.properties.name, { noHide: true })
                .on('mouseover', function () {
                    this.setStyle({
                        'opacity': 0.9,
                        'color': 'white'
                    });                    
                })
                .on('mouseout', function () {
                    this.setStyle(atollStyle);
                })
                .on('dblclick', function () {

                    layer.bindPopup('<p3>' + feature.properties.name + '</p3><br><p4>Immigration: ' + immigrants + '</p4><br><p4>Emigration: ' + emigrants + '</p4>').openPopup();
                })
                .on('click', function (e) {

                    layer.closePopup();
                    
                    for (var linkPath in linkLayers) {
                        var path = linkPath.split('-');

                        if (atollSelected != null && atollSelected != path[0]) {
                            map.addLayer(linkLayers[linkPath].feature);
                        }
                    }

                    if (atollSelected == null || atollSelected != feature.id) {

                        for (var linkPath in linkLayers) {
                            var path = linkPath.split('-');

                            if (feature.id != path[0] && feature.id != path[1]) {
                                map.removeLayer(linkLayers[linkPath].feature);
                            }
                            else {
                                if (feature.id == path[0]) emigrants += parseInt(linkLayers[linkPath].data.value);
                                if (feature.id == path[1]) immigrants += parseInt(linkLayers[linkPath].data.value);
                            }
                        }

                        atollSelected = feature.id;
                    }
                    else {
                        
                        atollSelected = null;
                    }
                    
                });

            atollLayers[feature.id] = {
                layer: layer,
                center: layer.getBounds().getCenter()
            };
        }, 
        style: function (feature) {
            return {
                stroke: 'black',
                weight: 1,
                opacity: 0.1
            }
        }
    }).addTo(map);
    
    // Check available viz
    for (var dataName in config.datasets) {
        // Draw buttons

        map.addControl(new datasetButton(dataName));

        if (config.datasets[dataName].hasOwnProperty('active') && config.datasets[dataName].active == true) {
            renderDataset(dataName);
        }
    }
    

    L.control.layers({}, { 'Terrain': osmMap, 'Satellite': esriMap, 'Watercolor': watercolor }).addTo(map);
});

function renderDataset(dataName) {
    for (var linkPath in linkLayers) {
        map.removeLayer(linkLayers[linkPath].feature);
    }

    linkLayers = {};

    var vizConfig = config.datasets[dataName];

    d3.csv(vizConfig.linkFile, function(error, links) {
        if (error) throw error;


        var valueMin = d3.min(links, function(l) { return (l.source != l.target) ? parseInt(l.value) : null; });
        var valueMax = d3.max(links, function(l) { return (l.source != l.target) ? parseInt(l.value) : null; });

        var valueScale = d3.scale.linear()
            .domain([valueMin, valueMax])
            .range([vizConfig.weightMin, vizConfig.weightMax]);

        var atollColors = [];
        var colorCategories = d3.scale.category20();
        for (var atoll in atollLayers) {
            atollColors[atoll] = colorCategories(Object.keys(atollColors).length);
        }

        var weightColors = d3.scale.ordinal()
            .domain([vizConfig.weightMin, (vizConfig.weightMax - vizConfig.weightMin) / 2, vizConfig.weightMax])
            .range(['red', 'blue', 'green']);

        links.forEach(function (link) {
            if (link.source != link.target) {

                var path = [ link.source, link.target ].join('-');
                var pathReverse = [ link.target, link.source ].join('-');

                var lineWeight = valueScale(link.value);

                if (vizConfig.hasOwnProperty('skipValueThreshold') && vizConfig.skipValueThreshold > link.value) {
                    return;
                }

                if (vizConfig.hasOwnProperty('skipWeightThreshold') && vizConfig.skipValueThreshold > lineWeight) {
                    return;
                }

                var lineColor = null;
                switch (vizConfig.lineColorMode) {
                    case 'fixed':
                        lineColor = (linkLayers.hasOwnProperty(pathReverse)) ? vizConfig.lineColorIn : vizConfig.lineColorOut;
                        break;
                    case 'atoll':
                        lineColor = atollColors[link.source];
                        break;
                    case 'weight':
                        lineColor = weightColors(lineWeight);
                        break;
                    case 'random':
                        lineColor = '#' + Math.floor(Math.random()*16777215).toString(16);
                        break;
                    default:
                        lineColor = vizConfig.lineColorMode;
                }

                var lineCenterLatLng = L.polyline([ atollLayers[link.source].center, atollLayers[link.target].center ])
                    .getBounds()
                    .getCenter();
                                
                var lineBreakLatLng = null;
                if (linkLayers[pathReverse]) {
                    lineBreakLatLng = L.latLng(
                        (lineCenterLatLng.lat * .001) + lineCenterLatLng.lat, 
                        (lineCenterLatLng.lng * .001) + lineCenterLatLng.lng
                    );
                }
                else {
                    lineBreakLatLng = L.latLng(
                        lineCenterLatLng.lat - (lineCenterLatLng.lat * .001), 
                        lineCenterLatLng.lng - (lineCenterLatLng.lng * .001)
                    );
                }
                
                var line = L.polyline(
                    [ 
                        atollLayers[link.source].center, 
                        lineBreakLatLng, 
                        atollLayers[link.target].center
                    ], 
                    {
                        color: lineColor,
                        weight: lineWeight,
                        smoothFactor: 1,
                        opacity: 0.5,
                        fill: false,
                        class: path
                    });

                var arrow = L.polylineDecorator(line, { patterns: [{
                        offset: '55%', 
                        symbol: L.Symbol.arrowHead({
                            polygon: true, 
                            pathOptions: {
                                weight: lineWeight,
                                color: lineColor
                            }
                        })
                    }]});

                var feature = L.featureGroup([line, arrow])
                    .bindPopup('<p3>' + link.source + '</p3> to <p3>' + link.target + '</p3>' + '<br>' + '<p4>' + link.value + ' migrants' + '</p4>')
                    .on('mouseover', function(e) {
                        if (config.linePopup == 'hover') {
                            this.openPopup();
                        }

                        this.setStyle({
                            opacity: 1
                        });
                    })
                    .on('mouseout', function(e) {
                        this.setStyle({
                            opacity: 0.5
                        });
                    })
                    .addTo(map);

                linkLayers[path] = {
                    feature: feature,
                    line: line,
                    arrow: arrow,
                    data: link
                }
            }
        });

    });
}

/**
 * Custom control to add a button
 */
var datasetButton =  L.Control.extend({
  dataName: null,
  options: { position: 'topright' },
  
  initialize: function(dataName) {
    this.dataName = dataName;
  },

  onAdd: function (map) {
    var self = this;
    var container = L.DomUtil.create('div', 'leaflet-bar ');

    container.innerText = this.dataName;
    container.style.background = '#fff';
    container.style.padding = '7px 15px';
    container.style.cursor = 'pointer';

    container.onclick = function(){
      renderDataset(self.dataName);
    }

    return container;
  }
});