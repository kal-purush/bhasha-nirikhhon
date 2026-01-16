import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';

@Component({
  selector: 'app-edit-email',
  templateUrl: './edit-email.component.html',
  styleUrls: ['./edit-email.component.less']
})
export class EditEmailComponent implements OnInit {

  constructor(private fb: FormBuilder) { }

  ngOnInit(): void {
  }

  @Input() emailToEdit: String;
  @Output() emailEmit = new EventEmitter<string>();
  @Output() cancelUpdate = new EventEmitter<void>();

  saveEmail(){
    
  }

  get email() {
    return this.emailForm.get('email');
  }

  emailForm: FormGroup = this.fb.group({
    email: ['', [Validators.required, Validators.pattern(/^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/) ]],
  });



  onSubmit() {
    if (this.emailForm.valid) {
      console.log('form submitted');
      this.emailEmit.emit('')
    } else {
      this.validateAllFormFields(this.emailForm);
    }
    console.log(this.emailForm.value);
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