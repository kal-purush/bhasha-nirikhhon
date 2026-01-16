import {Component, OnInit} from '@angular/core';
import {AuthService} from '../auth.service';
import {EditInvoice, InvoiceResponse, TokenModel, VisitResponse} from '../app.component';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import {ActivatedRoute, Router} from '@angular/router';
import * as $ from 'jquery';
@Component({
  selector: 'app-edit-invoice',
  templateUrl: './edit-invoice.component.html',
  styleUrls: ['./edit-invoice.component.css']
})
export class EditInvoiceComponent implements OnInit {
  private invoice: InvoiceResponse;
  private submitted = false;
  private editInvoiceForm: FormGroup;
  private invoiceId: number;
  private visits: VisitResponse[];

  constructor(private router: Router,
              private route: ActivatedRoute,
              private formBuilder: FormBuilder,
              private authService: AuthService) {
  }

  get f() {
    return this.editInvoiceForm.controls;
  }

  ngOnInit() {
    this.editInvoiceForm = this.formBuilder.group({
      companyName: ['', Validators.required],
      date: [null, Validators.required],
      discount: [0, [Validators.required, Validators.min(0)]]
    });
    const token: TokenModel = {accessToken: this.authService.getAccessToken()};
    this.authService.getEmployeesVisits(token).subscribe(data => {
      this.authService.setExpirationDate();
      this.visits = data.visits;
      this.visits.forEach(obj => {
        const date = new Date(parseInt(obj.visitDate.valueOf(), 10));
        obj.visitDate = date.getDate() + '.' + (date.getMonth() + 1) + '.' + date.getFullYear();
      });
    });
    this.invoiceId = parseInt(this.route.snapshot.paramMap.get('id'), 10);
    this.authService.getInvoice(this.invoiceId).subscribe((result) => {
      this.authService.setExpirationDate();
      this.invoice = result.invoice;
      const date = new Date(this.invoice.paymentDate);
      this.invoice.paymentDate = date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate();
    });
  }

  onSubmit() {
    this.submitted = true;
    const visit = $('#visit').val();
    const payment = $('#paymentMethod').val();
    if (this.editInvoiceForm.invalid || visit === '-1' || payment === '') {
      return;
    }
    const form: EditInvoice = {
      invoiceId: this.invoiceId,
      companyName: this.invoice.companyData.name,
      visitId: visit,
      paymentDate: this.invoice.paymentDate,
      methodOfPayment: payment,
      discount: this.invoice.discount,
      accessToken: this.authService.getAccessToken()
    };
    console.log(form);
    this.authService.editInvoice(form).subscribe((result) => {
      console.log(result);
    }, (result) => {
      console.log(result);
    });
  }
}