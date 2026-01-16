import { Component } from '@angular2/core';
import { PaymentTransactionService } from './PaymentTransaction.service';
import { Query } from './model/Query';
import {PaymentTransaction} from './model/PaymentTransaction';

@Component({
    selector: 'paymentTransaction',
    template: '<h1>Update transaction</h1>'
})
export class CreatePaymentTransactionComponent {

    constructor(private paymentTransactionService: PaymentTransactionService) {

    }

   	createPaymentTransaction() {
        let p = new PaymentTransaction();
        this.paymentTransactionService.createPaymentTransaction(p);
   	}
}