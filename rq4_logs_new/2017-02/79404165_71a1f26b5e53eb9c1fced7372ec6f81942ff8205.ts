import { Component, ViewChild } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import ol from 'openlayers';

/*
  Generated class for the Mapa page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-mapa',
  templateUrl: 'mapa.html'
})

export class MapaPage {

	ol: any;

	@ViewChild('map') map;

  constructor(public navCtrl: NavController, public navParams: NavParams) {}

  ionViewDidLoad() {
    console.log('ionViewDidLoad MapaPage');
    console.log(this.map);

     var projection = ol.proj.get('EPSG:3857');


     var map = new ol.Map({
            target: "map",
            layers: [
              new ol.layer.Tile({
                source: new ol.source.OSM() 
              })
            ],
            view: new ol.View({
              center: ol.proj.transform([8.92471, 46.07257], 'EPSG:4326', 'EPSG:3857'),
              zoom: 10
            })
          });
  }

}