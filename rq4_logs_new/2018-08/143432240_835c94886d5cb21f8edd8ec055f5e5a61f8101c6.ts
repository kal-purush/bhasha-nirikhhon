import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup } from '@angular/forms';

interface Login {
  login: string;
  password: string;
}

@Component({
  selector: 'app-form-control-sample',
  templateUrl: 'form-control.component.html'
})
export class FormControlComponent implements OnInit {

  // FormGroup - группа отдельных элементов управления (FormControl)
  // FormControl - класс, который представляет элемент управления
  loginForm: FormGroup;

  ngOnInit() {
    this.loginForm = new FormGroup({
      login: new FormControl('user1'),
      password: new FormControl()
    });
  }

  onSubmit(form) {
    console.log(form.valid);
    console.log(form.value);
  }
}