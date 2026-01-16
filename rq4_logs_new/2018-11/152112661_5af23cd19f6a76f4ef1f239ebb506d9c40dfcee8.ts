import { Component, OnInit } from '@angular/core';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import {CarBrandModel} from '../app.component';
import {AuthService} from '../auth.service';

@Component({
  selector: 'app-add-car-brand',
  templateUrl: './add-car-brand.component.html',
  styleUrls: ['./add-car-brand.component.css']
})
export class AddCarBrandComponent implements OnInit {

  private addCarBrandForm: FormGroup;
  constructor(private builder: FormBuilder,
              private service: AuthService) { }

  ngOnInit() {
    this.addCarBrandForm = this.builder.group({
      brandName: ['', Validators.required]
    });
  }

  get f() {
    return this.addCarBrandForm.controls;
  }

  submit() {
    if (this.f.brandName.errors) {
      return;
    }
    const model: CarBrandModel = {
      brandName: this.f.brandName.value,
      accessToken: this.service.getAccessToken()
    };
    this.service.addCarBrand(model);
  }
}