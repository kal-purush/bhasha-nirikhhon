

import { Component, ViewChild, ElementRef } from '@angular/core';
import { IonicPage } from 'ionic-angular';
import { Geolocation } from '@ionic-native/geolocation';


declare var google;

@Component({
  selector: 'page-how2reach',
  templateUrl: 'how2reach.html'
})
export class HowToReachPage {

  @ViewChild('map') mapElement: ElementRef;
  how2reach: string = "address";
  map: any;
  start = '';  // this should be current loaction
  end = "16.444317,74.968643";     // This should be destination(drop down select of famous mutts)
  directionsService = new google.maps.DirectionsService;
  directionsDisplay = new google.maps.DirectionsRenderer;

  constructor(private geolocation: Geolocation) {

    this.geolocation.getCurrentPosition().then((resp) => {
    if(resp){
      this.start = ""+resp.coords.latitude+","+resp.coords.longitude;
      this.calculateAndDisplayRoute();
      }
    }).catch((error) => {
      console.log('Error getting location', error);
    });

    //let watch = this.geolocation.watchPosition();
    //watch.subscribe((data) => {
     // data can be a set of coordinates, or an error (if an error occurred).
     // data.coords.latitude
     // data.coords.longitude
     //this.start = ""+resp.coords.latitude+","+resp.coords.longitude;
      //this.calculateAndDisplayRoute();
    //});
  }

  

  ionViewDidLoad(){
    this.initMap();
  }

  initMap() {
    var uluru = {lat: 16.444317, lng: 74.968643};
    this.map = new google.maps.Map(this.mapElement.nativeElement, {
      zoom: 8,
      center: uluru  // Mugalkhod mutt
    });
   
        var marker = new google.maps.Marker({
          position: uluru,
          map: this.map
        });

    this.directionsDisplay.setMap(this.map);
  }

  calculateAndDisplayRoute() {
    this.directionsService.route({
      origin: this.start,
      destination: this.end,
      travelMode: 'DRIVING'
    }, (response, status) => {
      if (status === 'OK') {
        this.directionsDisplay.setDirections(response);
      } else {
        window.alert('Directions request failed due to ' + status);
      }
    });
  }
  
}