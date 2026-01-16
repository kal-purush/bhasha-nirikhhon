import {Component, OnInit} from '@angular/core';
import {Customer} from './customer';
import {CustomerService} from "./customer.service";



@Component({
  selector: 'my-customers',
  templateUrl: './customer.component.html',
  styleUrls: ['./customer.component.css'],
  providers: [CustomerService]
})
export class CustomerComponent implements OnInit{
  // Customers = CUSTOMERS;
  customers: Customer[];
  selectedCustomer: Customer;

  constructor(private customerService : CustomerService){}

  getCustomers(): void{
    this.customerService.getCustomers().then(customers => this.customers = customers);
  }


  onSelect(customer: Customer): void {
    this.selectedCustomer = customer;
  }

  ngOnInit(): void{
    this.getCustomers();
  }

}



