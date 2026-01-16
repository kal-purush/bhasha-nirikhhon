
//REQUIRED DEPENDENCIES AND LIBRARIES
import { Component, ViewChild, ElementRef } from '@angular/core';
import { NavController } from 'ionic-angular';
import { FirebaseDatabase } from '../../classes/FirebaseDatabase';
import { SQLiteHandler } from '../../classes/SQLiteHandler';
import { LocationHandler } from '../../classes/LocationHandler';
import { DirectoryHandler } from '../../classes/DirectoryHandler';
import { UiProvider } from '../../providers/ui/ui';
import { Dates } from '../../classes/Dates';

declare var google: any;


//COMPONENT
@Component({
  selector: 'page-maps',
  templateUrl: 'maps.html'
})
 
//CLASS
export class MapsPage {

  @ViewChild('map') mapElement: ElementRef;
  map: any;

  //VARIABLE
  firebaseDatabaseObject = FirebaseDatabase.getInstance();
  sqliteObject = SQLiteHandler.getInstance();
  locationObject = LocationHandler.getInstance();
  directoryObject = DirectoryHandler.getInstance();
  datesObject = Dates.getInstance();


  //CONSTRUCTOR
  constructor(public navCtrl: NavController) {}

  ionViewDidLoad(){
    this.loadMap();
  }
 
  public loadMap(){
 
    let latLng = new google.maps.LatLng(-34.9290, 138.6010);
 
    let mapOptions = {
      center: latLng,
      zoom: 15,
      mapTypeId: google.maps.MapTypeId.ROADMAP
    }
 
    this.map = new google.maps.Map(this.mapElement.nativeElement, mapOptions);
 
  }

  public initMap() {
    var uluru = new google.maps.LatLng(39.305, -76.617);
    this.map = new google.maps.Map(document.getElementById('map'), {
      zoom: 4,
      center: uluru
    });

    var marker = new google.maps.Marker({
      position: uluru,
      map: this.map
    });
  }
}