import { Component } from '@angular/core';
import {
  FormBuilder,
  FormGroup,
  Validators
} from '@angular/forms';

@Component({
  selector: 'login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent {
  validateForm: FormGroup;
  
  constructor(private fb: FormBuilder) {
  }
  
  ngOnInit() {
    this.validateForm = this.fb.group({
      userId: [ null, [ Validators.required ] ],
      password: [ null, [ Validators.required ] ],
    });
  }
}