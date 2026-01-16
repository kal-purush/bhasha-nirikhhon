import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { Name } from 'src/app/interfaces/name.inteface';

@Component({
  selector: 'app-edit-name',
  templateUrl: './edit-name.component.html',
  styleUrls: ['./edit-name.component.less']
})
export class EditNameComponent implements OnInit {


  @Input() nameToEdit: String;
  @Output() nameEmit = new EventEmitter<Name>();
  @Output() cancelUpdate = new EventEmitter<void>();


  constructor(private fb: FormBuilder) { }

  ngOnInit(): void {
  }

  saveEmail(){
    
  }

  get firstname() {
    return this.nameForm.get('firstname');
  }
  get lastname() {
    return this.nameForm.get('lastname');
  }

  nameForm: FormGroup = this.fb.group({
    firstname: ['', [Validators.required]],
    lastname: ['', [Validators.required]],
  });



  onSubmit() {
    if (this.nameForm.valid) {
      console.log('form submitted');
      this.nameEmit.emit()
    } else {
      this.validateAllFormFields(this.nameForm);
    }
    console.log(this.nameForm.value);
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