import { Component } from '@angular/core';
import { NavController, NavParams,  AlertController } from 'ionic-angular';
import { Http, Headers } from '@angular/http';
import { AdminPage } from '../admin/admin';
import { Storage } from '@ionic/storage';

@Component({
  selector: 'page-add-item',
  templateUrl: 'add-item.html'
})
export class AddItemPage {
  public name: string;
  public description: string;
  public imageURL: string;
  public price: string;
  public calories: string;
  public category: string;
  public tags: string;

  constructor(public navCtrl: NavController,
    public navParams: NavParams, 
    private http: Http, 
    private alertCtrl: AlertController,
    private storage: Storage) {

    this.name= '';
    this.description= '';
    this.imageURL= '';
    this.price= '';
    this.calories= '';
    this.category= '';
    this.tags= '';
  }

  public addItem(): void {
    this.storage.get('token').then((value: string)=>{
      console.log(value);
    let link = 'https://keanubackend.herokuapp.com/admin/item/add'
    let header = new Headers({ 'Content-Type': 'application/json',  'token': value })
    let data =
      {
        'name': this.name,
        'description': this.description,
        'imageURL': this.imageURL,
        'price': this.price,
        'calories': this.calories,
        'category': this.category,
        'tags': this.tags,

      };
      console.log(data);
    this.http.post(link, data, header)
      .subscribe(
      data => { },
      err => {
        console.log('error')
        console.log(err);
      },
      () => {
        console.log('posted registration done')
        this.alertCtrl.create({
          title: 'Item Added',
          subTitle: 'Congratulations, a menu item has been added!',
          buttons: ['OK']
        }).present();
        this.navCtrl.setRoot(AdminPage);
      }
      )

  })
}
}