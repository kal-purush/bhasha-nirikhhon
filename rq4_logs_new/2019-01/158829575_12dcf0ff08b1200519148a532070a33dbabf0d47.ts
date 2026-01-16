import { Component } from '@angular/core';
import { AuthService } from 'src/app/services/auth.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-login-page',
  templateUrl: './login-page.component.html',
  styles: []
})
export class LoginPageComponent {
  public username: string;
  public password: string;
  private authService: AuthService;
  private router: Router;
  constructor(
    authService: AuthService,
    router: Router
    ) {
      this.authService = authService;
      this.router = router;
    }

  public onLogin(): void {
    this.authService.userLogin({
      username: this.username,
      id: Math.floor(Math.random() * 100)
    });
    this.router.navigate(['/about']);
  }
}