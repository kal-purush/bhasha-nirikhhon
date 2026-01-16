declare var Stripe;
import { Component } from '@angular/core';
import { NavController } from 'ionic-angular';
import { Http, Headers, Response} from '@angular/http';

import 'rxjs/add/operator/map';

/*
  Generated class for the Purchase page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-purchase',
  templateUrl: 'purchase.html'
})
export class PurchasePage {

  card = {};

  constructor(public navCtrl: NavController, public http:Http) {

  }

  ionViewDidLoad() {
    console.log('Hello PurchasePage Page');
  }
  logForm() {
    var data = {};
    var url = 'https://wdiconfapi.herokuapp.com/payment';
    Stripe.card.createToken(this.card, function(status, response) {
      if (response.error) {
        console.log("Error")
      }
      else {
        var token = response.id;
        var token_pass = { stripeToken: token}

        // new Promise(resolve => {
        //     this.http.post('http://wdiconfapi.herokuapp.com/payment',).subscribe(data => {
        //         if(data){
        //           console.log(data.json())
        //           if (data.json().success) {
        //             // console.log()
        //           }
        //           resolve(true);
        //         }
        //         else
        //           resolve(false);
        //           return (data.json);
        //     });
        // });


        var token = response.id;
        var token_pass = { stripeToken: token}
        var url = "http://wdiconfapi.herokuapp.com/payment"
        var headers = new Headers();
        headers.append('Content-Type', 'application/json');
        this.http.post('url',
          JSON.stringify(token_pass),
          { headers: headers })
          .map((res: Response) => {
            console.log(res);
            res.json()
          })
          .subscribe((res) => console.log("res", res));


        // var my_form = document.createElement('form');
        // var my_tb = document.createElement('input');
        // my_tb.name = 'stripeToken';
        // my_tb.value = token;
        // my_form.appendChild(my_tb);
        // this.http.post(url, my_form).map(res => res.json());
      }
    });
  }

}