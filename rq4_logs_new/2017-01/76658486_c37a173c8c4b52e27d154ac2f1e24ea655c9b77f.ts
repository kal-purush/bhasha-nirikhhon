import { Injectable, NgZone } from '@angular/core';
import { environment as env } from '../../environments/environment';
import { ApiService } from './api.service';
import { ReplaySubject, Observable } from 'rxjs';

@Injectable()
export class StripeService {

  constructor(private apiService: ApiService, private zone: NgZone) {
  }

  createCardToken(cardNumber: string, expiryMonth: string, expiryYear: string, cvc: string): Observable<string> {
    const requestSubject = new ReplaySubject<string>(1);

    (<any>window).Stripe.setPublishableKey(env.stripePubKey);

    (<any>window).Stripe.card.createToken({
      number: cardNumber,
      exp_month: expiryMonth,
      exp_year: expiryYear,
      cvc: cvc
    }, (status: number, response: any) => {
      this.zone.run(() => {
        if (status === 200) {
          requestSubject.next(response.id);
        } else {
          requestSubject.error("Error: " + response.error.message);
        }
      });
    });

    return requestSubject;
  }

  doPayment(token: string, amount: number): Observable<void> {
    return this.apiService.doPayment(amount, token);
  }

}