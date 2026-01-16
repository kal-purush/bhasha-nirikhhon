import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { AuthenticationService } from './authentication.service';
import { User } from './user.model';

@Component({
    moduleId: module.id,
    selector: 'my-login',
    templateUrl: 'login.component.html',
    providers: [ AuthenticationService ]
})

export class LoginComponent /*implements OnInit*/ {
    constructor(private router: Router, private authenticationService: AuthenticationService) { }

    //Member fields
    user: User;
    loading = false;

    //Member functions
    login() {
        this.loading = true;
        this.authenticationService.login(this.user.username, this.user.password)
            .subscribe(
                data => {
                    //Will eventually navigate user to either assessment or results depending on status
                    this.router.navigate(['/']);
                },
                error => {
                    this.loading = false;
                });
    }

    //Lifecycle methods
    ngOnInit() {
        // reset login status (will evenutally check to see here if a user is logged in (using authentication service) and if they are it will redirect them to assessment results page_
        this.authenticationService.logout();

        // create new user field
        this.user = new User;
    }

}