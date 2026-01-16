import { Component } from '@angular/core';

import { AuthService } from '@service/auth.service';

@Component({
  selector: 'app-logout',
  template: '',
  styles: [],
  providers: [AuthService],
})
export class LogoutComponent {
  constructor(private authService: AuthService) {
    this.authService.logout();
  }
}