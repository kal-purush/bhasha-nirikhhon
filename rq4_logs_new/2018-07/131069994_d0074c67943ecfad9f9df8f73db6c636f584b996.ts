import { Component} from '@angular/core';
import { NavController, NavParams, LoadingController, Events } from 'ionic-angular';
import { User } from '../../../_models/user';

@Component({
  selector: 'page-followers-bar',
  templateUrl: 'followers-bar.html',
})
export class FollowersBarPage{
  
  followers:User[] = [];

  constructor(
    public navCtrl: NavController, 
    public navParams: NavParams,
    private loadingController: LoadingController,
  ) {
    this.followers = this.navParams.get("followers");
    console.log(this.navParams.get("followers"));
  }

  loading(){
    let loading = this.loadingController.create({
      content: 'Por favor, aguarde...',
      spinner: 'bubbles'
    });

    loading.present();

    return loading;
  }

}