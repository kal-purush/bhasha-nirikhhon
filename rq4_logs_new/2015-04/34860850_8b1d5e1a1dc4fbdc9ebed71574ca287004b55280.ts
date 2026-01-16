declare var map: google.maps.Map;

Template['map'].rendered = function() {
    var mapOptions = {
        center: new google.maps.LatLng(55.6778724, 12.5623695),
        zoom: 14
    };

    map = new google.maps.Map(document.getElementById('map-canvas'),
                              mapOptions);

    Tracker.autorun(function() {
        Shops.find().forEach((shop) => {
            var marker = new google.maps.Marker({
                title: shop.name,
                position: new google.maps.LatLng(shop.lat, shop.lng),
            });

            marker.setMap(map);
        });
    });
};