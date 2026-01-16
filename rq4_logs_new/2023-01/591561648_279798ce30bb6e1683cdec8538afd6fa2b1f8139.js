// Location
let loc = $('#location_name');

// Long and Lat
let long = $('#longitude');
let lat = $('#latitude');

// Controll Map View
let map = L.map('map', {
    center: [41.505, -0.09],
    zoom: 15
});

// Make sure Map is Not Grey
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

// Make Pop up Appear
let popup = L.popup();

// Popup Function
onMapClick = (e) => {
    popup
        .setLatLng(e.latlng)
        .setContent("Lokasi Terpilih, silakan berikan nama")
        .openOn(map);

    // Change Value
    long.val(e.latlng.lng);
    lat.val(e.latlng.lat);
}

// Listener
map.on('click', onMapClick);
