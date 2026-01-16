//Global Variables
var map;
var pos;
var service;
var details;
var markers = [];

//Display's GPS map
function initMap() {
  map = new google.maps.Map(document.getElementById('map'), {
    center: pos,
    zoom: 10
  });
  var infoWindow = new google.maps.InfoWindow({map: map});

  //geolocation.
  if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(function(position) {
      pos = {
        lat: position.coords.latitude,
        lng: position.coords.longitude
      };

      infoWindow.setPosition(pos);
      infoWindow.setContent('You are here');
      map.setCenter(pos);
    }, function() {
      handleLocationError(true, infoWindow, map.getCenter());
    });
  } else {
    // Browser doesn't support Geolocation
    handleLocationError(false, infoWindow, map.getCenter());
  }
}

//if someone is using a really old browser
function handleLocationError(browserHasGeolocation, infoWindow, pos) {
  var infoWindow = new google.maps.InfoWindow({map: map});

  infoWindow.setPosition(pos);
  infoWindow.setContent(browserHasGeolocation ?
                        'Error: The Geolocation service failed :(' :
                        'Error: Your browser doesn\'t support geolocation, crap!');
}

//finds all the galleries
function findGalleries() {
  $(".markerButton").on("click", "button", function(event) {
    service = new google.maps.places.PlacesService(map);
    service.nearbySearch({
      location: pos,
      radius: radSend,
      type: ['art_gallery']
    }, callback);
  });
};

function setMapOnAll(map) {
  for (var i = 0; i < markers.length; i++) {
    markers[i].setMap(map);
  }
  markers = [];
}    

//takes info and goes through a loop for all results and calls createMarker
function callback(results, status) {
  if (status === google.maps.places.PlacesServiceStatus.OK) {
    setMapOnAll(null);
    for (var i = 0; i < results.length; i++) {
      createMarker(results[i]);
    }
  }
};

//puts markers on the map
function createMarker(place) {
  var placeLoc = place.geometry.location;
  var marker = new google.maps.Marker({
    map: map,
    position: place.geometry.location
  });

  markers.push(marker);

  //when you click on the marker it makes the name pop up
  google.maps.event.addListener(marker, 'click', function() {
    var infoWindow = new google.maps.InfoWindow({map: map, content: place.name});

    var pos = {
      lat: marker.position.lat(),
      lng: marker.position.lng()
    };

    infoWindow.setPosition(pos);
    //gets details of the specific location and puts the details in the sidebar
    var request = {placeId: place.place_id};
    
    service.getDetails(request, function(placeResult) {
      $('.mapinfo').html('<div class="js_gallery_info">' + '<div class="js_gallery_name">' + placeResult.name + '</div>' + 
      '<br>' + '<div class="js_gallery_address">' + placeResult.formatted_address + '</div>' + '<br>' + '<a href=' 
      + placeResult.website + '>' + placeResult.name + ' Website' + '</a>' + '</div>');
    });
  });  
};

//event listeners

$(function() {
  dropList();
})

$(function() {
  initMap();
});

$(function() {
  findGalleries();
});

$(function() {
  chooseDistance();
})

//make the dropdown list
/* When the user clicks on the button, 
toggle between hiding and showing the dropdown content */
function dropList() {
  $(".dropdown").on("click", "button", function(event) {    
    document.getElementById("myDropdown").classList.toggle("show");
  });
}

// Close the dropdown menu if the user clicks outside of it
window.onclick = function(event) {
  if (!event.target.matches('.dropbtn')) {

    var dropdowns = document.getElementsByClassName("dropdown-content");
    var i;
    for (i = 0; i < dropdowns.length; i++) {
      var openDropdown = dropdowns[i];
      if (openDropdown.classList.contains('show')) {
        openDropdown.classList.remove('show');
      }
    }
  }
}

//get user's choice for radius
var radSend;

function chooseDistance() {
  $(".choice").click(function(event) {
  var radChoice = document.getElementById("mySelect");
  radSend = radChoice.options[radChoice.selectedIndex].value; 
  });
};