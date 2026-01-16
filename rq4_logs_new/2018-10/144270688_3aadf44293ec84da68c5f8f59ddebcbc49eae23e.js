var x = document.getElementById("demo");
function getLocation() {
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(showPosition, showError);
        navigator.geolocation.getCurrentPosition(getJobs);
    } else {
        x.innerHTML = "Geolocation is not supported by this browser.";
    }
}
function showPosition(position) {
    var lat = position.coords.latitude;
    var lon = position.coords.longitude;
    var latlon = new google.maps.LatLng(lat, lon);
    var directionsService = new google.maps.DirectionsService();
    directionsDisplay = new google.maps.DirectionsRenderer();
    var city = new google.maps.LatLng(position.coords.latitude, position.coords.longitude);
    console.log(city);
    var mapOptions = {
        center: latlon,
        zoom: 7,
        mapTypeId: google.maps.MapTypeId.ROADMAP,
        mapTypeControl: false,
        navigationControlOptions: {
            style: google.maps.NavigationControlStyle.SMALL
        }
    }
    map = new google.maps.Map(document.getElementById("map_canvas"), mapOptions);
    directionsDisplay.setMap(map);
    var queryURL = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + position.coords.latitude + "," + position.coords.longitude + "&sensor=true";
    $.ajax({
        url: queryURL,
        method: 'GET'
    }).done(function (data) {
        var marker = new google.maps.Marker({
            position: latlon,
            map: map,
            title: "You are here!"
        });
    });
}
function getJobs(position) {
    var queryURL = "https://jobs.github.com/positions.json?lat=" + position.coords.latitude + "&long=" + position.coords.longitude + "";
    $.ajax({
        url: queryURL,
        method: 'GET',
        dataType: 'jsonp'
    }).done(function (data) {
        $.each(data, function (key, value) {
            console.log(key + ": " + value);
            let gig = '<li class="collection-item transparent"><a class= "white-text" href="' + value.url + '">' + value.company + ': ' + value.title;
            $('#joblist').append(gig);
        });
    });
}
function showError(error) {
    switch (error.code) {
        case error.PERMISSION_DENIED:
            x.innerHTML = "User denied the request for Geolocation."
            break;
        case error.POSITION_UNAVAILABLE:
            x.innerHTML = "Location information is unavailable."
            break;
        case error.TIMEOUT:
            x.innerHTML = "The request to get user location timed out."
            break;
        case error.UNKNOWN_ERROR:
            x.innerHTML = "An unknown error occurred."
            break;
    }
}
$(document).ready(function () {
    $('#getlocation').on('click', function () {
        getLocation();
    });
});