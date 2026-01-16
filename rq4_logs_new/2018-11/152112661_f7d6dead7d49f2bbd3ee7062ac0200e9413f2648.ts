import { Component, OnInit } from '@angular/core';
import {AcceptProFormaInvoice, InvoiceResponse} from '../app.component';
import {AuthService} from '../auth.service';
import {Router} from '@angular/router';

@Component({
  selector: 'app-get-pro-forma-invoices-list',
  templateUrl: './get-pro-forma-invoices-list.component.html',
  styleUrls: ['./get-pro-forma-invoices-list.component.css']
})
export class GetProFormaInvoicesListComponent implements OnInit {
  private invoices: InvoiceResponse[];
  constructor(private auth: AuthService,
              private  router: Router) { }

  ngOnInit() {
    this.auth.getInvoices('http://localhost:8080/warsztatZlomek/rest/invoice/getProFormaInvoicesList').subscribe((result) => {
      this.auth.setExpirationDate();
      this.invoices = result.invoices;
      this.invoices.forEach((invoice) => {
        const date = new Date(invoice.dayOfIssue);
        invoice.dayOfIssue = date.getDate() + '-' + (date.getMonth() + 1) + '-' + date.getFullYear();
        console.log(invoice);
      });
    });
  }
  save(i) {
    const form: AcceptProFormaInvoice = {
      accessToken: this.auth.getAccessToken(),
      proFormaInvoiceId: i
    };
    console.log(i);
    this.auth.acceptProFormaInvoice(form).subscribe((result) => {
      console.log(result);
    }, (result) => {
      console.log(result);
    });
  }

}