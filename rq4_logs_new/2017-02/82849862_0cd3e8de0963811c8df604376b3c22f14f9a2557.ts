import { Component } from '@angular/core';

import {Customer} from "../customer"
import {CustomerService} from "../customer.service"

@Component({
  selector: 'uncompleted-drafts ',
  templateUrl: './drafts.component.html'
})
export class DraftsComponent {

  customers: Customer[] = [];

  constructor (private customerService: CustomerService) {}



  ngOnInit(): void {
    function checkIfHasDraft(customer) {
      return customer.draft == true;
    }

    this.customerService.getCustomers()
        .then(customers => this.customers = customers.filter(checkIfHasDraft));
  }
}