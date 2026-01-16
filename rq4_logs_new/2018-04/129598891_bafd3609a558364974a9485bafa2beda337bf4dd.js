/**
 * This is a simple wrapper object around google maps so we can centralise any
 * functions without having to use thier API inline everywhere.
 */
const GoogleMaps = function() {

    this.render = function(mapEl, 
                           lat, 
                           long, 
                           storeData,
                           storeClickCallback) {

        console.log('Initialising google maps API with ', lat, long);
        const userLatLong = new google.maps.LatLng(lat, long);

        const mapOptions = {
        zoom: 15,
        center: userLatLong,
        mapTypeId: google.maps.MapTypeId.ROADMAP
        };
        console.log('Map options ', mapOptions);

        // Render the map into the DOM element
        const map = new google.maps.Map(mapEl, mapOptions);

        // Set up boundaries so the map zooms to the added kebab shops
        // and they're not off screen
        const bounds = new google.maps.LatLngBounds();
        bounds.extend(userLatLong);

        // Add the user marker to the map
        console.log('Adding user marker');
        const userMarker = new google.maps.Marker({
        position: userLatLong,
        map: map
        });

        // Add each kebab shop to the map for now
        console.log('Adding kebab shop markers');
        storeData.forEach(function (k) {

        const shopLatLong = new google.maps.LatLng(k.latitude, k.longitude);
        // Extend bounds dynamically for each shop
        bounds.extend(shopLatLong);

        console.log('Adding shop %s (%s) at %s,%s', k.name, k.id, k.latitude, k.longitude);
        const shopMarker = new google.maps.Marker({
        position: shopLatLong,
        title: k.name,
        label: k.name,
        map: map
        });

        // Handle click event for each marker
        shopMarker.addListener('click', function(event) {
        storeClickCallback(event, k.id);
        });

        });

        map.fitBounds(bounds);
        console.log('Finished map initialisation');

    }

};