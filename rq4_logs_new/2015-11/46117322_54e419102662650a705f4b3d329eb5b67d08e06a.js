function initMap() {
    geocoder = new google.maps.Geocoder();
    var myLatLng = {
        lat: -25.363,
        lng: 131.044
    };

    // Create a map object and specify the DOM element for display.
    map = new google.maps.Map(document.getElementById('map'), {
        center: myLatLng,
        zoom: 15
    });
    var infoWindow = new google.maps.InfoWindow({
        map: map
    });
}


function getLocationFromString(address) {
    geocoder.geocode({
            'address': address
        },
        function(results, status) {
            if (status == google.maps.GeocoderStatus.OK) {
                map.setCenter(results[0].geometry.location);
            } else {
                //DO nothing
            }
        }
    );
}

function getLocation() {
    // Try HTML5 geolocation.
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(function(position) {
            var pos = {
                lat: position.coords.latitude,
                lng: position.coords.longitude
            };

            infoWindow.setPosition(pos);
            infoWindow.setContent('Location found.');
            map.setCenter(pos);
        }, function() {
            handleLocationError(true, infoWindow, map.getCenter());
        });
    } else {
        // Browser doesn't support Geolocation
        handleLocationError(false, infoWindow, map.getCenter());
    }
}

function handleLocationError(browserHasGeolocation, infoWindow, pos) {
    infoWindow.setPosition(pos);
    infoWindow.setContent(browserHasGeolocation ?
        'Error: The Geolocation service failed.' :
        'Error: Your browser doesn\'t support geolocation.');
}

function fixWindowHeight() {
    var sectionHeight = $(window).height() - ($("header").outerHeight() + $("footer").outerHeight());
    $(".content").height(sectionHeight);
}

(function() {
    var map = null;
    initMap();
    getLocation();
    // Fix window height
    fixWindowHeight();
    $(window).resize(function() {
        fixWindowHeight();
    })
})();