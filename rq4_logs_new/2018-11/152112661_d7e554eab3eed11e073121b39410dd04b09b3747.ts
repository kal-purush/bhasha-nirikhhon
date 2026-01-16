import {Component, OnInit} from '@angular/core';
import {AuthService} from '../auth.service';
import {ClientCompany, CompanyModel} from '../app.component';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import * as $ from 'jquery';

@Component({
  selector: 'app-add-client-to-company-form',
  templateUrl: './add-client-to-company-form.component.html',
  styleUrls: ['./add-client-to-company-form.component.css']
})
export class AddClientToCompanyFormComponent implements OnInit {
  private submitted = false;
  private addClientToCompanyForm: FormGroup;
  private companies: CompanyModel[] = [];

  constructor(private auth: AuthService,
              private builder: FormBuilder) {
  }

  ngOnInit() {
    this.auth.getAllCompanies().subscribe((result) => {
      this.auth.setExpirationDate();
      this.companies = result.companies;
    }, (result) => {
      console.log(result);
    });
    this.addClientToCompanyForm = this.builder.group({
      email: ['', [Validators.required, Validators.pattern('^[A-Za-z0-9._-]{1,}@[a-z0-9]{1,6}.[a-z]{2,3}$')]]
    });
  }

  get f() {
    return this.addClientToCompanyForm.controls;
  }

  onSubmit() {
    this.submitted = true;
    const companyName = $('#select').val();
    if (this.addClientToCompanyForm.invalid || companyName === '' || companyName === undefined) {
      return;
    }
    const form: ClientCompany = {
      accessToken: this.auth.getAccessToken(),
      companyName: companyName,
      username: this.f.email.value
    };
    this.auth.addClientToCompany(form);
  }
}