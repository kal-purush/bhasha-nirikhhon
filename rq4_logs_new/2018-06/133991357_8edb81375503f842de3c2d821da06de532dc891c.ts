import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators, NG_ASYNC_VALIDATORS } from '@angular/forms';
import CustomValidators from '../forms/CustomValidators';

@Component({
  selector: 'register-company',
  templateUrl: './registerc.component.html',
  styleUrls: ['./registerc.component.css']
})
export class RegisterCompanyComponent implements OnInit {
  register_companyForm: FormGroup;
  constructor(private formBuilder: FormBuilder) {}

  ngOnInit() {
    this.register_companyForm = this.formBuilder.group({
      name: ['',[Validators.required, Validators.minLength[5]]],
      username: ['',[Validators.required, Validators.minLength[5]]],
      email: ['', [Validators.required, CustomValidators.validateEmail]],
      password:['', Validators.required, Validators.minLength[6]],
      phone:['', Validators.required, CustomValidators.length[10]],
      adress: ['', Validators.required, Validators.minLength[10]]

    });
  }
  submitForm(): void {
    console.log(this.register_companyForm);
  }
}