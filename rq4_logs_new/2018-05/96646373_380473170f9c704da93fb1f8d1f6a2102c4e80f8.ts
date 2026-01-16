import { Component, ViewChild, ElementRef } from '@angular/core';
import { NavController, NavParams } from "ionic-angular";

import leaflet from 'leaflet';

/**
 * Generated class for the MapPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@Component({
  selector: "page-map",
  templateUrl: "map.html"
})
export class MapPage {
  @ViewChild('map') mapContainer: ElementRef;
  map: any;
  public places:Array<string> = ['Toronto', 'A', 'B'];
  public index:number = 1;

  constructor(public navCtrl: NavController, public navParams: NavParams) {}

  ionViewDidEnter() {
    this.loadMap();
  }

  previous() {
    this.index = (this.index - 1) % this.places.length;
    document.getElementById('map-box').innerHTML = this.places[this.index];
    
  }

  next() {
    this.index = (this.index + 1) % this.places.length;
    document.getElementById('map-box').innerHTML = this.places[this.index];
  }

  loadMap() {
    this.map = leaflet.map("map", {
      zoomControl: false
    }).fitWorld();
    leaflet.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attributions: 'Leaflet',
      maxZoom: 18
    }).addTo(this.map);
  }
}