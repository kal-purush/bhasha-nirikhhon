import { Component, Input } from '@angular/core';
import { Customer} from "./customer"

@Component({
  selector: 'customer-details',
  template: `
  <div *ngIf="customer">
    <h1>Kliento duomenys:</h1>
    <h2>{{customer.name}}</h2>
    <div><label>Suma: </label> €{{customer.loan}}</div>
    <div><label>Grynosios mėnesio pajamos: </label> €{{customer.income}}</div>
    <div><label>Statusas: </label>{{customer.status}}</div>
    
    <button (click)="acceptLoan()">
      Patvirtinti
    </button>
    <button (click)="declineLoan()">
      Atmesti
    </button>
  </div>`
})
export class CustomerDetailsComponent {
  @Input()
  customer: Customer;
}