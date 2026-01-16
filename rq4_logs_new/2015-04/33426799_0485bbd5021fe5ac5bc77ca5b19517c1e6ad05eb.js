// # Place all the behaviors and hooks related to the matching controller here.
// # All this logic will automatically be available in application.js.
// # You can use CoffeeScript in this file: http://coffeescript.org/
$(document).ready(function () {

  var map;
    function initialize() {
      var myLatlng = new google.maps.LatLng(55.9532520,-3.1882670);
      var mapOptions = {
        zoom: 12,
        center: myLatlng,
        draggable: true,
        disableDefaultUI: true
      }
      var locations = data
      console.log('data', data)
      var myLatlng;
      var map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);
      centerControlDiv = document.createElement('div');
      findMeDiv = document.createElement('div'); 
      centerControl = new CenterControl(centerControlDiv, map)
      findMe = new FindMe(findMeDiv, map)
      centerControlDiv.index = 1;
      findMeDiv.index = 1;
      map.controls[google.maps.ControlPosition.BOTTOM_LEFT].push(centerControlDiv);
      map.controls[google.maps.ControlPosition.BOTTOM_RIGHT].push(findMeDiv);

      google.maps.event.addDomListener(window, 'resize', function() {
        myLatlng = new google.maps.LatLng(55.9532520,-3.1882670);
        map.setCenter(myLatlng)
        google.maps.event.trigger(map, 'resize');
      });
      var infowindow = new google.maps.InfoWindow;
      var markers = [];
      var marker, i;
      for (i = 0; i < locations.length; i++) {  
        marker = new google.maps.Marker({
             title: locations[i][0],
             position: new google.maps.LatLng(locations[i][1], locations[i][2]),
             map: map
        });
      google.maps.event.addListener(marker, 'click', (function(marker, i) {
           return function() {
               var contentString = "<p>" + locations[i][0] + " is a " + locations[i][3] + " located at " + locations[i][7] + " " +locations[i][9]+ " " + locations[i][8] + ". It is open from " + locations[i][5] + " until " + locations[i][6] + ". </p>"
               infowindow.setContent(contentString);
               infowindow.open(map, marker);
           }
      })(marker, i));
    markers.push(marker);
    }
    if(navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(function(position) {
        var pos = new google.maps.LatLng(position.coords.latitude,
                                         position.coords.longitude);
        $('.spinner').remove()
        getFiveNearestVenues(map, markers, pos)
      }, function() {
        handleNoGeolocation(true);
      });

    } else {
      // Browser doesn't support Geolocation
      handleNoGeolocation(false);
    }
  }

  function getFiveNearestVenues(map, markers, pos) {
    console.log('markers at start', markers)
    if(!pos || !markers){
      return;
    }
    $.each(markers, function(){
      var distance = google.maps.geometry.spherical.computeDistanceBetween(this.getPosition(), pos);
      this.distance = distance
      console.log('this marker', this)
    });
    markers.sort(function(a,b){
      return a.distance - b.distance;
    })
    console.log('markers after', markers)
    nearest = markers.slice(0,6)

    $.each(nearest, function(){
      list_item = document.createElement('li');
      list_item.setAttribute("id", this.title);
      console.log('list_item', list_item)
      distance_in_miles = (this.distance * 0.000621371192).toFixed(2);
      console.log('google maps', google.maps)
      console.log('got li by name', document.getElementById(this.title))
      list_item.innerHTML = this.title + " " + "(" + distance_in_miles + " miles away" + ")";
      $('#sidebar').append(list_item);
      console.log('this position', this.getPosition())
    })
    markers.filter(function(marker){
      console.log('list_item id in filter', list_item.id)
      console.log('marker.title', marker.title)
      if (marker.title == list_item.id){var result = marker}
      return result
    })
    google.maps.event.addDomListener(document.getElementById(result.title), "click", function(ev) {
      console.log('result', result)
      map.setCenter(result);
    });

  };

  function CenterControl(controlDiv, map) {
    // Set CSS for the control border
    var controlUI = document.createElement('div');
    controlUI.style.backgroundColor = '#fff';
    controlUI.style.border = '2px solid #fff';
    controlUI.style.borderRadius = '3px';
    controlUI.style.boxShadow = '0 2px 6px rgba(0,0,0,.3)';
    controlUI.style.cursor = 'pointer';
    controlUI.style.marginBottom = '22px';
    controlUI.style.textAlign = 'center';
    controlUI.title = 'Click to recenter the map';
    controlDiv.appendChild(controlUI);

    // Set CSS for the control interior
    var controlText = document.createElement('div');
    controlText.style.color = 'rgb(25,25,25)';
    controlText.style.fontFamily = 'Roboto,Arial,sans-serif';
    controlText.style.fontSize = '16px';
    controlText.style.lineHeight = '38px';
    controlText.style.paddingLeft = '5px';
    controlText.style.paddingRight = '5px';
    controlText.innerHTML = 'Show Me Dog Venues';
    controlUI.appendChild(controlText);

    // Setup the click event listeners: simply set the map to
    // Edinburgh
    google.maps.event.addDomListener(controlUI, 'click', function() {
      myLatlng = new google.maps.LatLng(55.9532520,-3.1882670);
      map.setCenter(myLatlng)
    });
  };

  function FindMe(findMeDiv, map) {
    // Set CSS for the control border
    var controlUI = document.createElement('div');
    controlUI.style.backgroundColor = '#fff';
    controlUI.style.border = '2px solid #fff';
    controlUI.style.borderRadius = '3px';
    controlUI.style.boxShadow = '0 2px 6px rgba(0,0,0,.3)';
    controlUI.style.cursor = 'pointer';
    controlUI.style.marginBottom = '22px';
    controlUI.style.textAlign = 'center';
    controlUI.title = 'Click to recenter the map';
    findMeDiv.appendChild(controlUI);

    // Set CSS for the control interior
    var controlText = document.createElement('div');
    controlText.style.color = 'rgb(25,25,25)';
    controlText.style.fontFamily = 'Roboto,Arial,sans-serif';
    controlText.style.fontSize = '16px';
    controlText.style.lineHeight = '38px';
    controlText.style.paddingLeft = '5px';
    controlText.style.paddingRight = '5px';
    controlText.innerHTML = '<span style="padding: 0 10px;">Find Me</span><i class="fa fa-location-arrow" style="color: rgb(79, 154, 255); padding: 0 10px;"></i>';
    controlUI.appendChild(controlText);

    // Setup the click event listeners: simply set the map to
    // Edinburgh
    google.maps.event.addDomListener(controlUI, 'click', function() {
      if(navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(function(position) {
        var pos = new google.maps.LatLng(position.coords.latitude,
                                         position.coords.longitude);
        var infowindow = new google.maps.InfoWindow({
          map: map,
          position: pos,
          content: 'You are here!'
        });
        map.setCenter(pos);
      }, function() {
        handleNoGeolocation(true);
      });
    } else {
      // Browser doesn't support Geolocation
      handleNoGeolocation(false);
    }
      // var pos = new google.maps.LatLng(55.9532520,-3.1882670);
      // map.setCenter(myLatlng)
    });
  };

    
  google.maps.event.addDomListener(window, 'load', initialize(function(){

  })); 
})