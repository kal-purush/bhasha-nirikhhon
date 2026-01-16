import { Component } from '@angular/core';
import { Router } from '@angular/router';
import { Http } from '@angular/http';

import { UsersService } from './../../../core/services/users.service';

@Component({
    selector: 'app-login',
    templateUrl: './login.component.html',
    styleUrls: [ './login.component.css' ],
    providers: [ UsersService ]
})
export class LoginComponent {
    constructor(
        private usersService: UsersService,
        private router: Router,
        private http: Http
    ) { }

    login(event, username, password) {
        event.preventDefault();
    }
}