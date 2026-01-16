import { Component, OnInit } from '@angular/core';
import { AlertService } from '../shared/alert/alert.service';
import { AuthService } from '../core/auth/auth.service';

@Component({
  selector: 'cm-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']
})
export class LoginComponent implements OnInit {

  username: string;
  password: string;

  admin = {
    username: 'admin',
    password: 'admin',
    token: 'asdgerhadfhbvefhergafgsthsgjfghsdfgsadtawrtehjnhlksklneidufhghdinfkfgdnfkgjdnfgkjdnf'
  };

  constructor(
    private alert: AlertService,
    private authService: AuthService
  ) { }

  ngOnInit() {
  }

  submit () {
    if (!this.username || !this.password) {
      this.alert.error('Form invalid!');
    }

    if (this.username !== this.admin.username) {
      return this.alert.error('Username or pasword are not correct');
    }

    if (this.password !== this.admin.password) {
      return this.alert.error('Username or pasword are not correct');
    }
    this.authService.login(this.admin.username, this.admin.token);
  }
}