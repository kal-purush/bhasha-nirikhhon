import {IonicApp, Page, NavController, Events, NavParams, Alert} from 'ionic-angular';
import {FormBuilder, Validators, FORM_BINDINGS, ControlGroup} from 'angular2/common'

import {HttpService} from '../../providers/http-service';
import {ValidationService} from '../../providers/validator-service';

@Page({
  providers: [HttpService, ValidationService],
  templateUrl: 'build/pages/checkout/checkout.html'
})
export class CheckoutPage {

  form: ControlGroup;
  meetup: any;
  loading: boolean;

  constructor(formBuilder: FormBuilder, public nav: NavController, public http: HttpService, public navParams: NavParams) {
    this.nav = nav;
    this.http = http;

    this.meetup = navParams.get('meetup');
    // Build form with validators
    this.form = formBuilder.group({
           number: ["", ValidationService.cardNumber],
           cvc: ["", ValidationService.cvc],
           exp: ["", ValidationService.expiry]
       });
  }

  onCheckout() {
    console.log(this.form.valid);

    if (this.form.valid) {
      this.loading = true;
      // build the request from the form
      var exp = this.form.value.exp.split('/');
      var token = Stripe.createToken({
          number: this.form.value.number,
          cvc: this.form.value.cvc,
          exp_month: exp[0],
          exp_year: exp[1]
      }, (status, response) => {
          if (response.error || !response.id) {
              let popup = Alert.create({
                  title: 'Payment Error',
                  subTitle: 'Please try again',
                  buttons: ['Dismiss']
              });
              this.loading = false;
              this.nav.present(popup);
          } else {
            let request = { id: this.meetup.id, token : response.id}
            this.http.makeBackendRequest('POST', 'meetup/payement', request,
            response => {
                HttpService.showAlert(this.nav, "Its all good !", "All invitations has been sent, you can cancel the meetup in 'My meetups'", "Ok");
                this.nav.popToRoot();
            }, errorMessage => {
              let code = errorMessage.status;
              if (typeof code == "undefined")
                  HttpService.showAlert(this.nav, "Serveur non-accessible", "Notre serveur n'a pas répondu, veuillez réessayez", "Ok");
              else if (code == "500")
                  HttpService.showAlert(this.nav, "Serveur non-accessible", "Notre serveur n'a pas répondu, veuillez réessayez", "Ok");
              else
                  HttpService.showAlert(this.nav, "Serveur non-accessible", "Notre serveur n'a pas répondu, veuillez réessayez", "Ok");
            }, true);
          }
      });
    }
  }


}