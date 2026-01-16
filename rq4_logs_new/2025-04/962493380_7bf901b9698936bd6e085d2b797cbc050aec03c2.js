const map = L.map('map').setView([56.8389, 60.6057], 13);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: 'Map data © OpenStreetMap contributors'
}).addTo(map);

// Вместо fetch — прямо сюда вставляем данные:
const buildings = {
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "name": "Телебашня",
        "description": "Символ города, демонтирован в 2018 году."
      },
      "geometry": {
        "type": "Point",
        "coordinates": [60.6149, 56.8326]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "name": "Дом Ярутина",
        "description": "Историческое здание, снесено в 2009 году."
      },
      "geometry": {
        "type": "Point",
        "coordinates": [60.6087, 56.8341]
      }
    }
  ]
};

L.geoJSON(buildings, {
  onEachFeature: function (feature, layer) {
    layer.on('click', () => {
      const props = feature.properties;
      layer.bindPopup(`<strong>${props.name}</strong><br>${props.description}`).openPopup();
    });
  },
  pointToLayer: function (feature, latlng) {
    return L.circleMarker(latlng, {
      radius: 6,
      fillColor: 'red',
      color: 'darkred',
      weight: 1,
      fillOpacity: 0.8
    });
  }
}).addTo(map);