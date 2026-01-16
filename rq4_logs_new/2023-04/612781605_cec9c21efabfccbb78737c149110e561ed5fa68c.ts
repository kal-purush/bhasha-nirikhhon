import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';

@Component({
  selector: 'app-edit-password',
  templateUrl: './edit-password.component.html',
  styleUrls: ['./edit-password.component.less']
})
export class EditPasswordComponent implements OnInit {

  constructor(private fb: FormBuilder) { }

  ngOnInit(): void {
  }

  @Output() passwordEmit = new EventEmitter<string>();
  @Output() cancelUpdate = new EventEmitter<void>();

  savePassword(){
    this.passwordEmit.emit('')
  }


  get oldPassword() {
    return this.passwordForm.get('oldPassword');
  }
  get newPassword() {
    return this.passwordForm.get('newPassword');
  }
  get confirmPassword() {
    return this.passwordForm.get('confirmPassword');
  }

  passwordForm: FormGroup = this.fb.group({
    oldPassword: ['', [Validators.required]],
    newPassword: ['', [Validators.required, Validators.pattern(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$/)]],
    confirmPassword: ['', [Validators.required]]
  });



  onSubmit() {
    if (this.passwordForm.valid) {
      console.log('form submitted');
      this.passwordEmit.emit()
    } else {
      this.validateAllFormFields(this.passwordForm);
    }
    console.log(this.passwordForm.value);
  }

  onCancel(){
    this.cancelUpdate.emit()
  }


  validateAllFormFields(formGroup: FormGroup) {
    Object.keys(formGroup.controls).forEach(field => {
      const control = formGroup.get(field);
      if (control instanceof FormControl) {
        control.markAsTouched({ onlySelf: true });
      } else if (control instanceof FormGroup) {
        this.validateAllFormFields(control);
      }
    });
  }
}