import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';
/*
  Generated class for the ArtikelBaca page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-artikel-baca',
  templateUrl: 'artikel-baca.html'
})
export class ArtikelBacaPage {
  id: any;
  posts: any;

  constructor(public navCtrl: NavController, public navParams: NavParams, public http: Http) {
  	this.id = navParams.data;

  	http.get('http://cybex.agri.web.id/api/artikel_b.php?idartikel='+this.id).map(res => res.json()).subscribe(data => {
        this.posts = data;
    }); 

  }

  ionViewDidLoad() {
    console.log('Hello ArtikelBacaPage Page');
  }

}