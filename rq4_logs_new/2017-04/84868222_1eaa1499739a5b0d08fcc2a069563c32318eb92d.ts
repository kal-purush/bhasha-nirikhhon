import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

/*
  Generated class for the SpotDetail page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/

@Component({
  selector: 'page-spot-detail',
  templateUrl: 'spot-detail.html'
})

export class SpotDetailPage {
	spotId: number;
	spot: Object;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
  	this.spotId = navParams.get('id');

  	let spotDetails = {
  		12345: {'name': 'Escher Gärtchen', 'description': 'Das kleine Gärtchen an der Escherstr.', 'rating': 3.5, 'owner': 'Franz53', 'img': ["assets/img/spot_image.png", "assets/img/spot_image.png", "assets/img/spot_image.png"], 'city': 'Köln', 'cityrank': 66, 'lat': 50.955639, 'lng': 6.948812 },
		12346: {'name': 'Wickraths Strauß', 'description': 'Blümchen! Schöne Blumen zu jeder Jahreszeit.', 'rating': 4.5, 'owner': 'Sandra', 'img': ["assets/img/spot_image.png", "assets/img/spot_image.png", "assets/img/spot_image.png"], 'city': 'Köln', 'cityrank': 13, 'lat': 50.954375, 'lng': 6.952084},
		12347: {'name': 'Kasparle', 'description': 'Gelbe Pflänzlein bevorzugt.', 'rating': 3.5, 'owner': 'Günni', 'img': ["assets/img/spot_image.png", "assets/img/spot_image.png", "assets/img/spot_image.png"], 'lat': 50.953486, 'city': 'Köln', 'cityrank': 65, 'lng': 6.953065},
		12348: {'name': 'Ringschen', 'description': 'Direkt am Hansaring: die kleine Oase.', 'rating': 5, 'owner': 'Haxn Herb', 'img': ["assets/img/spot_image.png", "assets/img/spot_image.png", "assets/img/spot_image.png"], 'city': 'Köln', 'cityrank': 1, 'lat': 50.949931, 'lng': 6.955110}
	};
  	this.spot = spotDetails[this.spotId];
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad SpotDetailPage');
  }

  ratingChange(event: any) {
  	console.log('Rated something, tbd');
  }

}