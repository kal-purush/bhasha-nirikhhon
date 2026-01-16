import { Component } from '@angular/core';
import { FormGroup, ControlValueAccessor, FormControl, FormArray, FormBuilder, Validators, AbstractControl } from '@angular/forms';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'app';

  regForm = new FormGroup({
    id: new FormControl(''),
    fullName: new FormGroup({
      first: new FormControl(),
      last:  new FormControl(),
    }),
    phones: new FormArray([])
  });

  constructor(fb: FormBuilder) {
     this.regForm = fb.group({
       id :['', [Validators.maxLength(5), v(5)]]
     })
  }
}

function v(param){
  return function v(c: AbstractControl) {
      // if(param)
     return { text: 'Something went wrong!' }
     return null;
  }
}


/*
FormCotrol
FormGroup
FormArray

FormBuilder

- [ ]
- [ ]
+

*/