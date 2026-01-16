import {
	Component,
	ElementRef,
	ViewChild
} from '@angular/core';
import {
	NavController
} from 'ionic-angular';

declare var google;

@Component({
	selector: 'page-map',
	templateUrl: 'map.html'
})
export class MapPage {

	@ViewChild('map') mapElement: ElementRef;

	map: any;
	mapInitialised: boolean = false;
	apiKey: string = "AIzaSyBu9-88Bzcq4LlXqeQgXgT77iCW5q6X5Gw";

	constructor(public navCtrl: NavController) {
		this.loadGoogleMaps();
	}

	loadGoogleMaps() {

		if (typeof google == "undefined" || typeof google.maps == "undefined") {

			console.log("online, loading map");

			//Load the SDK
			window['mapInit'] = () => {
				this.initMap();
				this.enableMap();
				this.showMarkers();
			}

			let script = document.createElement("script");
			script.id = "googleMaps";

			if (this.apiKey) {
				script.src = 'http://maps.google.com/maps/api/js?key=' + this.apiKey + '&callback=mapInit';
			} else {
				script.src = 'http://maps.google.com/maps/api/js?callback=mapInit';
			}

			document.body.appendChild(script);

		} else {


			console.log("showing map");
			this.initMap();
			this.enableMap();
			this.showMarkers();

		}

	}

	initMap() {

		this.mapInitialised = true;

		let centerLatLng = new google.maps.LatLng(50.954375, 6.952084);

		let mapOptions = {
			center: centerLatLng,
			zoom: 15,
			mapTypeId: google.maps.MapTypeId.ROADMAP
		}

		this.map = new google.maps.Map(this.mapElement.nativeElement, mapOptions);

	}

	disableMap() {
		console.log("disable map");
	}

	enableMap() {
		console.log("enable map");
	}

	showMarkers() {
		let spotMarker = 'assets/img/spotmarker.png';

		let markers = [{'id': 12345, 'name': 'Escher Gärtchen', 'lat': 50.955639, 'lng': 6.948812},
						{'id': 12346, 'name': 'Wickraths Strauß', 'lat': 50.954375, 'lng': 6.952084},
						{'id': 12347, 'name': 'Kasparle', 'lat': 50.953486, 'lng': 6.953065},
						{'id': 12348, 'name': 'Ringschen', 'lat': 50.949931, 'lng': 6.955110},];

		markers.forEach((marker) => {
			console.log(marker);
			let pos = new google.maps.LatLng(marker['lat'], marker['lng']);
			let m = new google.maps.Marker({
				position: pos,
				icon: spotMarker,
				title: marker['name']
			});
			m.setMap(this.map);
		});
	}

}