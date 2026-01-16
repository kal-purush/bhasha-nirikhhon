import { Component, OnInit } from '@angular/core';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import {User} from '../../models/user.model';

@Component({
  selector: 'app-edit-info',
  templateUrl: './edit-info.component.html',
  styleUrls: ['./edit-info.component.css']
})
export class EditInfoComponent implements OnInit {
  user: User;
  newUser: User;
  repeatedPassword: string;
  somePlaceholder: string;
  angForm: FormGroup;
  constructor(private fb: FormBuilder) { }

  ngOnInit() {
    this.user = JSON.parse(localStorage.getItem('user'));
    this.newUser = new User();
    this.somePlaceholder = 'new value';
  }

  get f() { return this.angForm.controls; }

  private createForm() {
    this.angForm = this.fb.group({
      firstname: ['', [Validators.required]],
      lastname: ['', [Validators.required]],
      email: ['', [Validators.required, Validators.email]],
      password: ['', [Validators.required, Validators.minLength(6)]],
      address: ['', [Validators.required]],
      city: ['', [Validators.required]],
      country: ['', [Validators.required]],
      ssn: ['', [Validators.required, Validators.minLength(13), Validators.maxLength(13)]],
      phonenumber: ['', [Validators.required, Validators.minLength(9), Validators.maxLength(10)]],
      repeatpassword: ['', Validators.required]
    });
  }
  onSubmit() { }
}