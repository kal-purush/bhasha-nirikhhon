var map;
function initMap() {
  map = new google.maps.Map(document.getElementById('map'), {
    center: {lat: 13.002297, lng: 77.569378},
    zoom: 8
  });
}