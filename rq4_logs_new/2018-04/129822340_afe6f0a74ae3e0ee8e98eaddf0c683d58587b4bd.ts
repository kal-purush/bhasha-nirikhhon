import { Component, OnInit } from '@angular/core';
import {AuthService} from '../../services/auth.service';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent {
    email: string = '';
    password: string = '';

    constructor(public authService: AuthService) {}

    signup() {
        if (!(this.email && this.password)) { return; }

		this.authService.signup(this.email, this.password);
		this.resetFields();
	}

	login() {
        if (!(this.email && this.password)) { return; }

		this.authService.login(this.email, this.password);
        this.resetFields();
	}

	resetFields() {
		this.email = this.password = '';
	}
}