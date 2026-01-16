
import { Component, OnInit, Renderer } from '@angular/core';

@Component({
  selector: 'app-payment',
  templateUrl: './payment.component.html',
  styleUrls: ['./payment.component.css']
})
export class PaymentComponent implements OnInit {
  globalListener: any;

  constructor(private renderer: Renderer) {}

  ngOnInit() {
  }

  openCheckout() {
    var handler = (<any>window).StripeCheckout.configure({
      key: 'sk_test_avZQTtZbzC7UiORO5J1cQ8IU',
      locale: 'auto',
      token: function (token: any) {
        // You can access the token ID with `token.id`.
        // Get the token ID to your server-side code for use.
      }
    });

    handler.open({
      name: 'Demo Site',
      description: '2 widgets',
      amount: 2000
    });

    this.globalListener = this.renderer.listenGlobal('window', 'popstate', () => {
      handler.close();
    });
  }
  
  ngOnDestroy() {
    this.globalListener();
  }
}
