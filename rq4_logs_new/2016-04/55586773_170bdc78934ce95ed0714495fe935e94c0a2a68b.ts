import {Component} from "angular2/core";
import {UserService} from "./../services/UserService";
import {Router} from "angular2/router";
import {ROUTER_DIRECTIVES} from "angular2/router";

@Component({
    selector: 'login-form',
    templateUrl: './templates/_login.html',
    directives: [ ROUTER_DIRECTIVES]
})

export class LoginFormComponent {
    userService : UserService;

    login : string = "Enter Your login";
    password: string = "";

    submitted:boolean = false;

    constructor(userService : UserService) {
        this.userService = userService;
    }
    onSubmit( ){
        this.submitted = true;
        console.log("Bilou");


        this.userService.initSession(this.login, this.password)
            .subscribe(() => {
                console.log("Success");
            });

    }

}