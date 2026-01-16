import './routing.css'
import * as vietmapGl from "@vietmap/vietmap-gl-js";
import "@vietmap/vietmap-gl-js/vietmap-gl.css";
import React, { useEffect, useRef, useState } from "react";
import dataJSON from '../data.json';
import dataLine from '../line.json';

function LineWithPopup() {
    const mapContainer = useRef(null);
    const map = useRef(null);
    const vietmap = { lng: 106.632997, lat: 10.799550 };
    const VIETMAP_API_KEY = "YOUR_API_KEY_HERE";
    const zoom = 14;
    const [data, setData] = useState([]);
    const [line, setLine] = useState([]);
    const markers = useRef([]);
    const lineLayers = useRef([]);

    useEffect(() => {
        setData(dataJSON);
        setLine(dataLine);
        map.current = new vietmapGl.Map({
            container: mapContainer.current,
            style: "https://maps.vietmap.vn/mt/tm/style.json?apikey=" + VIETMAP_API_KEY,
            center: [vietmap.lng, vietmap.lat],
            zoom: zoom,
        })
        map.current.addControl(new vietmapGl.NavigationControl(), 'top-right');
        map.current.on('click', (e) => 
            map.current.queryRenderedFeatures(e.point).forEach((feature) => {

                if (feature.layer.id.includes("line-layer")) {
                    console.log(e.lngLat) ;
                    console.log("Line clicked");
                    new vietmapGl.Popup({ closeOnClick: true })
                    .setLngLat(e.lngLat)
                    .setHTML("<h4>Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.</h4>")
                    .setMaxWidth("300px")
                    .addTo(map.current);
                }else{
                    console.log("Map clicked");
                }
            }
        ));
    
    });

    const addMarkers = (locations) => {
        if (markers.current.length > 0) {
            markers.current.forEach((marker) => {
                marker.remove(); 
            });
            markers.current = [];
        }
        locations.forEach((location) => {
            const marker = new vietmapGl.Marker()
                .setLngLat([location.lng, location.lat])
                .addTo(map.current);
    
            markers.current.push(marker); 
    
        });
    };
    
    const decodePolyline = (encoded) => {
        const points = [];
        let index = 0,
            len = encoded.length;
        let lat = 0,
            lng = 0;
    
        while (index < len) {
            let byte, shift = 0, result = 0;
    
            do {
                byte = encoded.charCodeAt(index++) - 63;
                result |= (byte & 0x1f) << shift;
                shift += 5;
            } while (byte >= 0x20);
    
            const dlat = result & 1 ? ~(result >> 1) : result >> 1;
            lat += dlat;
    
            shift = 0;
            result = 0;
            do {
                byte = encoded.charCodeAt(index++) - 63;
                result |= (byte & 0x1f) << shift;
                shift += 5;
            } while (byte >= 0x20);
    
            const dlng = result & 1 ? ~(result >> 1) : result >> 1;
            lng += dlng;
    
            points.push([lng / 1e5, lat / 1e5]); 
        }
    
        return points;
    };
    
    const addLine = (lineData) => {
        if (lineLayers.current.length > 0) {
            lineLayers.current.forEach((layer) => {
                if (map.current.getLayer(layer.id)) {
                    map.current.removeLayer(layer.id);
                    map.current.removeSource(layer.source);
                }
            });
            lineLayers.current = [];
        }
    
        const lineCoordinates = decodePolyline(lineData.line); 
    
        const lineSource = {
            type: "geojson",
            data: {
                type: "Feature",
                geometry: {
                    type: "LineString",
                    coordinates: lineCoordinates,
                },
            },
        };
    
        map.current.addSource(`line-source-${lineData.id}`, lineSource);
    
        const lineLayer = {
            id: `line-layer-${lineData.id}`,
            source: `line-source-${lineData.id}`,
            type: "line",
            paint: {
                "line-color": "#FF0000",
                "line-width": 4,      
            },
        };
    
        map.current.addLayer(lineLayer);
        lineLayers.current.push(lineLayer);

        let minLng = Infinity, maxLng = -Infinity, minLat = Infinity, maxLat = -Infinity;

        lineCoordinates.forEach(([lng, lat]) => {
            if (lng < minLng) minLng = lng;
            if (lng > maxLng) maxLng = lng;
            if (lat < minLat) minLat = lat;
            if (lat > maxLat) maxLat = lat;
        });
    
        const bounds = [[minLng, minLat], [maxLng, maxLat]];
    
        map.current.fitBounds(bounds, {
            padding: 50,
        });
        
    };
    

    const handleButton1Click = () => {
        const locations = data.filter((item) => item.address === 1 || item.address === 2);
        const lineData = line.find((line) => line.id === 1); 
        addMarkers(locations);
        addLine(lineData);
    }

    const handleButton2Click = () => {
        const locations = data.filter((item) => item.address === 2 || item.address === 3);
        const lineData = line.find((line) => line.id === 2); 
        addMarkers(locations);
        addLine(lineData);
    }

    return (
        <div>
            <div ref={mapContainer} className="map"></div>
            <button onClick={handleButton1Click} className="map-button">
                Button 1
            </button>
            <button onClick={handleButton2Click} className="map-button">
                Button 2
            </button>
        </div>
    );
}

export default LineWithPopup;