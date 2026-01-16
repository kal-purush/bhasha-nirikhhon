import { Component } from '@angular/core';
import { NavController } from 'ionic-angular';

@Component({
  selector: 'page-contact',
  templateUrl: 'contact.html'
})
export class ContactPage {
 movies :[{
  movie : string,
  geners: string,
  release : string
}];

  constructor(public navCtrl: NavController) {
    this.movies = [
      {
          movie : "The Transporter",
          geners: "Action",
          release : "August 23"
      }
    ]
    this.movies.push({
      movie : "Sherlock Holmes",
      geners: "Suspense",
      release : "August 23"
    },{
      movie : "Minions",
      geners: "Comedy animation",
      release : "August 23"
    },{
      movie : "Step Up - All In",
      geners: "Dancing",
      release : "August 23"
    })
  };
}