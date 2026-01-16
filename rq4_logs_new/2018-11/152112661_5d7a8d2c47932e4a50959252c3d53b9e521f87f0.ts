import { Component, OnInit } from '@angular/core';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import {AuthService} from '../auth.service';
import {AddServiceForm} from '../app.component';
import {map} from 'rxjs/operators';

@Component({
  selector: 'app-add-service',
  templateUrl: './add-service.component.html',
  styleUrls: ['./add-service.component.css']
})
export class AddServiceComponent implements OnInit {
  private addServiceForm: FormGroup;
  private submitted = false;
  constructor(private builder: FormBuilder,
              private auth: AuthService) { }

  ngOnInit() {
      this.addServiceForm = this.builder.group({
        name: ['', Validators.required],
        tax: [0, [Validators.min(0), Validators.max(25)]]
      });
  }

  get f() {
    return this.addServiceForm.controls;
  }

  onSubmit() {
    this.submitted = true;
    if (this.addServiceForm.invalid) {
      return;
    }
    const form: AddServiceForm = {
      accessToken: this.auth.getAccessToken(),
      name: this.f.name.value,
      tax: this.f.tax.value
    };
    this.auth.addService(form).subscribe((result) => {
      console.log(result);
      this.auth.setExpirationDate();
    }, (result) => {
      console.log(result);
    })
  }
}