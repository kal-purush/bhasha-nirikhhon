import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import { Storage } from '@ionic/storage';

@Component({
  selector: 'page-settings',
  templateUrl: 'settings.html',
})
export class SettingsPage {

  scale: any;
  theme: any;

  constructor(public navCtrl: NavController, public navParams: NavParams, public storage: Storage) {
  }

  ionViewDidLoad(){
    this.getSettings();
  }

  getSettings(){
    this.storage.get('scale').then((val) => {
      this.scale = val;
    });
    if(this.scale == null){
      console.log('empty scale');
      this.storage.set('scale', 'far');
      this.scale = 'far';
    }      
    this.storage.get('theme').then((val) => {
      this.theme = val;
    });
    if(this.theme == null){
      console.log('empty theme');
      this.storage.set('theme', 'light');
      this.theme = 'light';
    }
    console.log(this.scale);
    console.log(this.theme);
  }

  scaleChange(scale) {
    this.storage.set('scale', scale);
  }

  themeChange(theme) {
    this.storage.set('theme', theme);
  }

}