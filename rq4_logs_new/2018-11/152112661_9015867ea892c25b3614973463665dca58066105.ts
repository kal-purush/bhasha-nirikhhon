import { Component, OnInit } from '@angular/core';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import {AuthService} from '../auth.service';
import {BanUser} from '../app.component';

@Component({
  selector: 'app-ban-user',
  templateUrl: './ban-user.component.html',
  styleUrls: ['./ban-user.component.css']
})
export class BanUserComponent implements OnInit {
  private banUserForm: FormGroup;
  constructor(private formBuilder: FormBuilder,
              private authService: AuthService) {}

  ngOnInit() {
    this.banUserForm = this.formBuilder.group({
    email: ['', Validators.required]
  });
  }
  get f() {
    return this.banUserForm.controls;
  }

  onSubmit() {
    const banUser: BanUser = {
      username:  this.f.email.value,
      accessToken: this.authService.getAccessToken()
    };
  this.authService.banUserRequest(banUser);
  }
}