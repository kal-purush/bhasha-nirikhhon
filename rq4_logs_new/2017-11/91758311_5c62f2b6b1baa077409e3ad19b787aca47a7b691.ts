import { Injectable } from '@angular/core';
import { Platform, Events } from 'ionic-angular';
import 'rxjs/add/operator/map';
import { Geolocation, GeolocationOptions } from '@ionic-native/geolocation';
import { Storage } from '@ionic/storage';

/*
  Generated class for the GeofenceProvider provider.
*/

declare var google;

interface geofence{
  x: number;
  y: number;
  radius: number;
}

@Injectable()
export class GeofenceProvider {

  public geofence: geofence;
  public geolocationOptions: GeolocationOptions = {enableHighAccuracy: true};

  constructor(private platform: Platform, public storage: Storage, private geolocation: Geolocation, private events: Events) {
    this.platform.ready().then(() => {
      console.log('Hello GeofenceProvider Provider');
      this.initGeofence();
    });
  }

  public updateGeofence(): void {
    console.log("Saving geofence to storage", this.geofence);
    this.storage.set('geofence', this.geofence);
  }

  private initGeofence(): void {
    console.log("In ititGeofence");
    this.storage.get('geofence').then(storageGeofence => {
      if(storageGeofence){
        this.geofence = storageGeofence;
        this.events.publish('setupGeofence:runInBackground');
        console.log("Got the geofence from the DB", this.geofence);
      }
    }).catch(error => {
      console.log("Not able to get the geofence from storage");
    });
  }

  private deg2rad(deg): number {
    return deg * (Math.PI/180)
  }

  // See if the current position is in the geofence
  public currentlyInGeofence(): Promise<boolean>{
    console.log("Seeing if a point is currently in the geofence");
    return this.geolocation.getCurrentPosition(this.geolocationOptions).then(location => {
      /*      
      //var distance = Math.sqrt(Math.pow((location.coords.latitude - this.geofence.x), 2) + Math.pow((location.coords.longitude - this.geofence.y), 2));
      var R = 6371.009; // Radius of the earth in km
      //var R = 6367444.65712259;
      var differenceLatitude = this.deg2rad(location.coords.latitude - this.geofence.x);  // deg2rad below
      var differenceLongitute = this.deg2rad(location.coords.longitude - this.geofence.y); 
      var a = 
        Math.sin(differenceLatitude/2) * Math.sin(differenceLatitude/2) +
        Math.cos(this.deg2rad(this.geofence.x)) * Math.cos(this.deg2rad(location.coords.latitude)) * 
        Math.sin(differenceLongitute/2) * Math.sin(differenceLongitute/2)
        ; 
      var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
      var d = R * c; // Distance in km
      var distance = d;
      //var distance = d * (1000); // convert km to meters
    
      console.log("Distance: ", distance);
      console.log("Radius: ", this.geofence.radius);
      console.log("Accuracy: ", location.coords.accuracy);
      console.log("Latitude: " + location.coords.latitude + " Longitude: " + location.coords.longitude);
      console.log("x: " + this.geofence.x + " y: " + this.geofence.y);
      if(distance < this.geofence.radius){
        return true;
      } else {
        return false;
      }*/
      let accuracyOffset: number = 10;
      let latLng = new google.maps.LatLng(location.coords.latitude, location.coords.longitude); 
      let circleLatLng = new google.maps.LatLng(this.geofence.x, this.geofence.y);       
      var distance = google.maps.geometry.spherical.computeDistanceBetween(circleLatLng, latLng);
      console.log("Distance: ", distance);
      console.log("Radius: ", this.geofence.radius);
      console.log("Accuracy: ", location.coords.accuracy);
      if(distance < this.geofence.radius + accuracyOffset){
        return true;
      } else {
        return false;
      }
    });
  }
}