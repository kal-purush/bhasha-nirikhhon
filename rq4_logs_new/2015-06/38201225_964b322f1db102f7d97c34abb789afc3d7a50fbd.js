$(document).ready(function() {
    //Invoke ZIP Code Search API
    $("#btnSearch").click(function() {
        zipcode = $("#searchCriteria").val();
        var xhr = new XMLHttpRequest({mozSystem: true});
        xhr.open("GET", "http://api.zippopotam.us/us/" + zipcode, true);
        xhr.onreadystatechange = function() {
            if (xhr.readyState == 4) {
                var result = "";
                var resp = JSON.parse(xhr.response);
                var location_obj = resp.places[0];
                result += "<li>Latitude: " + location_obj['latitude'] + "</li>";
                result += "<li>Longitude: " + location_obj['longitude'] + "</li>";
                result += "<li>City: " + location_obj['place name'] + "</li>";
                result += "<li>State: " + location_obj['state'] + "</li>";
                console.log(result);
                $("#searchResults").html(result);
                $('#searchResults').listview('refresh');
            }
        }
        xhr.send();
    });
});