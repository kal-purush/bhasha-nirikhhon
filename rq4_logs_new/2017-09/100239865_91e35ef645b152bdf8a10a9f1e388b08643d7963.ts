import { Component } from '@angular/core';
import {
  FormBuilder,
  FormGroup,
  Validators
} from '@angular/forms';

@Component({
  selector: 'register',
  templateUrl: './register.component.html',
  styleUrls: ['./register.component.scss']
})
export class RegisterComponent {
  validateForm: FormGroup;

  constructor(private fb: FormBuilder) {
  }

  ngOnInit() {
    this.validateForm = this.fb.group({
      userId: [ null, [ Validators.required ] ],
      userName: [ null, [ Validators.required ] ],
      password: [ null, [ Validators.required ] ],
    });
  }
}