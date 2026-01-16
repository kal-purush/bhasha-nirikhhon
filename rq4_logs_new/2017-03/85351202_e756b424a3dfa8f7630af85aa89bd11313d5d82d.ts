import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

import { RestNote } from '../../models/rest-note';

import { HttpRestNotes } from '../../providers/http-rest-notes';

/*
  Generated class for the NoteDetails page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-note-details',
  templateUrl: 'note-details.html'
})
export class NoteDetailsPage {
  note: RestNote;

  constructor(public navCtrl: NavController, public navParams: NavParams, private restNotes: HttpRestNotes) {}

  ionViewDidLoad() {
    console.log('ionViewDidLoad NoteDetailsPage');
  }

}





// @Component({
//   selector: 'page-user-details',
//   templateUrl: 'user-details.html'
// })
// export class UserDetailsPage {
//     user: User;
//     login: string;

//   constructor(public navCtrl: NavController, public navParams: NavParams, private githubUsers: GithubUsers) {
//     this.login = navParams.get('login');
//     githubUsers.loadDetails(this.login).subscribe(user => {
//       this.user = user;
//     })
//   }

//   ionViewDidLoad() {
//     console.log('Hello UserDetails Page');
//   }

// }