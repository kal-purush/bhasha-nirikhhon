import {Component, Input, OnInit} from '@angular/core';
import {Router} from '@angular/router';
import {AuthenticationService} from '../service/authentication.service';

@Component({
  selector: 'app-login-register-page',
  templateUrl: './login-page.component.html',
  styleUrls: ['./login-page.component.css']
})
export class LoginPageComponent implements OnInit {

  loginUsername = '';
  loginPassword = '';
  errorMessage = 'Invalid Credentials. Please try again';
  invalidLogin = false;

  constructor(
    private router: Router,
    public authenticationService: AuthenticationService) { }

  ngOnInit(): void {
  }

  handleJWTAuthLogin = () => {
    this.authenticationService.executeJWTAuthenticationService(this.loginUsername, this.loginPassword)
      .subscribe(
        response => {
          console.log(response);
          this.router.navigate([`main/${this.loginUsername}`]);
          this.invalidLogin = false;
        },
        error => {
          console.log(error);
          this.invalidLogin = true;
        }
      );
  }
}