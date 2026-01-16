function myMap() {
    var options = {
        zoom : 10,
        center : {lat: 45.0809, lng: 14.5926}
    };

    var map = new google.maps.Map(document.getElementById('map'), options);
    
    
    function get_markers(json, icon, link) {
        $.getJSON(json, function(data) { 
            $.each(data, function(i, value) {
                var myLatlng = new google.maps.LatLng(value.lat, value.lng);
                var marker = new google.maps.Marker({
                    position: myLatlng,
                    map: map,
                    icon: (icon)
                });
                var infoWindow = new google.maps.InfoWindow({
                    content: '<a class="normal_text" href="' + link + value.id + '"><p>' + value.name + '</p>\n\
                            <img src="' + value.path + '" alt="' + value.name + '" style="width:150px"></a>'
                });
                marker.addListener('click', function(){
                    infoWindow.open(map, marker);
                    setTimeout(function () { infoWindow.close(); }, 2000);
                });
            });
        });
    };
    
    get_markers('ajax/accomm_markers.php', 'img/markers/lodging-2.png', 'apartment.php?id=');
    
    var coords = new google.maps.LatLng(45.080136, 14.456912);
    var marker = new google.maps.Marker({
       position: coords,
       map: map,
       icon: 'img/markers/malnar.png'
    });
    var infoWindow = new google.maps.InfoWindow({
        content: '<a class="normal_text" href="http://www.krktourist.com">\n\
                  <img src="img/project/official_logo.png" alt="Malnar-Gabor" style="width:150px"/></a>'
    });
    marker.addListener('click', function(){
        infoWindow.open(map, marker);
        setTimeout(function () { infoWindow.close(); }, 2000);
    });
}
