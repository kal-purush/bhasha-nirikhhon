import { Component } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';

@Component({
  selector: 'app-forgotPass',
  templateUrl: './forgot-pass.component.html',
  styleUrls: ['./forgot-pass.component.scss'],
})
export class ForgotPassComponent {
  constructor(private formBuilder: FormBuilder) { }

  formForget: FormGroup = this.formBuilder.group({
    email: ['', [Validators.email]],
  });

  onSubmit() {
    if (this.formForget.valid) {
      alert(JSON.stringify(this.formForget.value));
    }
  }
}