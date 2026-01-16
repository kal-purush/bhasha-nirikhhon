import {Component, OnInit} from '@angular/core';
import {Customer} from './customer';
import {CustomerService} from "./customer.service";



@Component({
  selector: 'my-customers',
  template: `
<div class="panel panel-default">
  <div class="panel-heading"><h1 style="text-align:center">Vartojimo paskolos</h1></div>
  

<table class="table table-striped" >
  <thead class="thead-inverse">
  <tr>
    <th>Suma</th>
    <th>Vartotojas</th>
    <th>Statusas</th>
    <th>Data</th>
    
  </tr>
  </thead>
  <tbody>
  <tr *ngFor="let customer of customers" 
    [class.selected]="customer === selectedCustomer" 
    (click) = "onSelect(customer)">
      <td  >
        €{{customer.loan}}
      </td>    
      <td  >
        {{customer.name}}
      </td>    
      <td  >
        {{customer.status}}
      </td>    
      <td  >  
        {{customer.date}}
      </td>
  </tr>  
  
  </tbody>
</table>
</div>


<customer-details [customer]="selectedCustomer"></customer-details>



<hr>
<div class="panel panel-default">
  <div class="panel-footer"><h1 style="text-align:center"></h1></div>
</div>

`,
  styleUrls: ['./app.component.css'],
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



