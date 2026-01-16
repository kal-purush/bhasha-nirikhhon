function Rooms(serverAdress) {
    this.serverAdress = serverAdress;
    this.isConnected = false;
    this.mapCenter = new google.maps.LatLng(54.19265, 16.1779);
    this.markers = {};
    this.myMarker = null;
    this.choosenMarker = {marker: null}
    this.previousSelectedMarker = null;
    this.map = null;
    geocoder = new google.maps.Geocoder();
    this.connect();
    this.initialize();
}

Rooms.prototype = {
    connect: function() {
        this.ws = new WebSocket(this.serverAdress)
        this.ws.onopen = this.onConnect.bind(this);
        this.ws.onmessage = this.onMessage.bind(this);
        this.ws.onclose = this.onDisconnect.bind(this);
    },

    onConnect: function() {
        this.isConnected = true;
    },

    onDisconnect: function() {
        this.ws.close();
        this.isConnected = false;
    },

    onMessage: function(evt) {
        var data = JSON.parse(evt.data);
        if (data.type === "AddedNewRoom") {
            var id = data.id;
            var title = data.title;
            var lat = data.lat;
            var lng = data.lng;
            var position = new google.maps.LatLng(lat, lng)
            this.addMarker(id, position, title);
        }
    },

    sendWebSocket: function (object) {
        this.ws.send(JSON.stringify(object));
    },

    toggleBounce: function (marker) {
        if (marker.getAnimation() !== null) {
            marker.setAnimation(null);
        } else {
            marker.setAnimation(google.maps.Animation.BOUNCE)

        }
    },

    clickMarkerFunction: function(marker) {
        if (this.choosenMarker.marker === null) {
            this.choosenMarker.marker = marker;
            this.choosenMarker.marker.setAnimation(google.maps.Animation.BOUNCE)
        } else if ((this.choosenMarker.marker.getAnimation() !== null) && (this.choosenMarker.marker.idRoom === marker.idRoom)) {
            this.choosenMarker.marker.setAnimation(null);
        } else if ((this.choosenMarker.marker.getAnimation() !== null) && (this.choosenMarker.marker.idRoom !== marker.idRoom)) {
            this.choosenMarker.marker.setAnimation(null);
            this.choosenMarker.marker = marker;
            this.choosenMarker.marker.setAnimation(google.maps.Animation.BOUNCE)
        } else if ((this.choosenMarker.marker.getAnimation() === null) && (this.choosenMarker.marker.idRoom === marker.idRoom)) {
            this.choosenMarker.marker.setAnimation(google.maps.Animation.BOUNCE)
        } else if ((this.choosenMarker.marker.getAnimation() === null) && (this.choosenMarker.marker.idRoom !== marker.idRoom)) {
            this.choosenMarker.marker = marker;
            this.choosenMarker.marker.setAnimation(google.maps.Animation.BOUNCE)
        }
        marker.infowindow.open(this.map,marker);
    },

    initialize: function ()  //function initializes Google map
    {
        var googleMapOptions =
        {
            center: this.mapCenter, // map center
            zoom: 4, //zoom level, 0 = earth view to higher value
            streetViewControl: false,
            panControl: true, //enable pan Control
            zoomControl: true, //enable zoom control
            zoomControlOptions: {
                style: google.maps.ZoomControlStyle.SMALL //zoom control size
            },
            scaleControl: true, // enable scale control
            mapTypeId: google.maps.MapTypeId.ROADMAP // google map type
			
        };
        this.map = new google.maps.Map(document.getElementById("map"), googleMapOptions);
    },

    addMarker: function (id, position, title) {
        var marker = new google.maps.Marker({
            position: position,
            map: this.map,
            draggable:false,
            title: title,
            icon: "http://maps.google.com/mapfiles/ms/micons/red.png"
        });
        marker.idRoom = id;

        var contentString = String(marker.idRoom);
        marker.infowindow = new google.maps.InfoWindow({
            content: contentString
        });

        google.maps.event.addListener(marker, 'click', this.clickMarkerFunction.bind(this, marker));
    },

    addMyMarker: function ()
    { //function that will add markers on button click
        var marker = new google.maps.Marker({
            position: this.mapCenter,
            map: this.map,
            draggable:true,
            animation: google.maps.Animation.DROP,
            title:"New room",
            icon: "http://maps.google.com/mapfiles/ms/micons/purple.png"
        });

        this.myMarker = marker;
        var total;
        google.maps.event.addListener(marker, "dragend", function () {
            var lat, lng, address;
            geocoder.geocode({ 'latLng': marker.getPosition() }, function (results, status) {
                if (status == google.maps.GeocoderStatus.OK) {
                    lat = marker.getPosition().lat();
                    lng = marker.getPosition().lng();
                    address = results[0].formatted_address;
                    total = ("<strong>\nAddress:</strong> " + address);
                    //return total;
                }
            });
            document.getElementById("RoomDescription").innerHTML = (total);
        });
    },

    deleteMyMarker: function() {
        this.myMarker.setMap(null);
    },

    confirmNewRoom: function() {
        if (this.myMarker != null) {
            var lat = this.myMarker.getPosition().lat();
            var lng = this.myMarker.getPosition().lng();
            var title = this.myMarker.getTitle();
            this.sendWebSocket({type: "AddNewRoom", lat: lat, lng: lng, title: title})
            this.deleteMyMarker()
        }
    }
}