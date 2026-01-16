import {Component} from 'angular2/core'
import {AuthenticationService} from '../../services/authentication-service'

@Component({
    selector: 'etik-tak-login-form',
    template: `
        <h1>Log ind</h1>
        <form (submit)="login(username.value, password.value)">
            <input #username class="form-control" placeholder="Brugernavn" required="required" type="text" /> <br />
            <input #password class="form-control" placeholder="Adgangskode" required="required" type="password" /> <br />
            <button type="submit" class="btn btn-default">Log ind</button>
        </form>
    `
})

export class LoginComponent {

    constructor(private authenticationService: AuthenticationService) {}

    login(username: string, password: string) {
        this.authenticationService.login(username, password);
    }
}