// Initialize and add the map
function initMap() {
    // The location of Uluru
    const newDelhi = { lat: 28.6139, lng: 77.2090 };
    // The map, centered at Uluru
    const map = new google.maps.Map(document.getElementById("map-container"), {
      zoom: 4,
      center: newDelhi,
    });
    // The marker, positioned at Uluru
    const marker = new google.maps.Marker({
      position: newDelhi,
      map: map,
    });
  }