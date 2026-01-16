import { Component, ViewChild} from '@angular/core';
import { PaymentTransaction} from '../model/PaymentTransaction';
import { PaymentTransactionItem} from '../model/PaymentTransactionItem';
import {ListCategoriesComponent} from '../components/listcategories.component'
import { PaymentTransactionService } from '../service/PaymentTransaction.service';
//import { Query} from './Query';

@Component({
    selector: 'simple2',
    directives: [ListCategoriesComponent],
    templateUrl: 'App/editpaymenttransaction.html'
    providers: [PaymentTransactionService]
})
export class EditPaymentTransactionComponent {
	 paymentTransaction = new PaymentTransaction();
	 item = new PaymentTransactionItem();
	 @ViewChild(ListCategoriesComponent) 
	 listCategories:ListCategoriesComponent;
	 
	 constructor(private paymentTransactionService: PaymentTransactionService) {
	 }
	 
	 save(){
		 this.paymentTransactionService.save(this.paymentTransaction);
	 }
	 
	 addItem(){
		 this.item.category=this.listCategories.selected;
		 this.paymentTransaction.items.push(this.item);
		 this.item = new PaymentTransactionItem();
	 }
	
	  // TODO: Remove this when we're done
	  get diagnostic() { return JSON.stringify(this.paymenTransaction); }
}