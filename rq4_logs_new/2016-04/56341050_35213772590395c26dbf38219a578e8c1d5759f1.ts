import {Component} from 'angular2/core';
import {RouteParams} from 'angular2/router';

@Component({
  selector: 'google-map',
  template: require('./google-map.html'),
  styles: [require('./google-map.scss')]
})

export class GoogleMapCmp {
  lat: number;
  lng: number;

  constructor(
    private params: RouteParams
  ) {
    this.lat = parseInt(params.get('lat'), 10);
    this.lng = parseInt(params.get('lng'), 10);
  }

  ngOnInit() {
    const map_lat = this.lat;
    const map_lng = this.lng;

    (function initMap() {
      const coords = {lat: map_lat, lng: map_lng};
      // Create a map object and specify the DOM element for display.
      const map = new google.maps.Map(document.getElementById('map'), {
        center: coords,
        scrollwheel: false,
        zoom: 5
      });
    })()
  }

}