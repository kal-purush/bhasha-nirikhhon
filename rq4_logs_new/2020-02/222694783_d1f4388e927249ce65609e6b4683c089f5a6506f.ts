import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { User } from '../../models/user.model';
import {ChangePassService} from './changePass.service';

@Component({
  // tslint:disable-next-line:component-selector
  selector: 'app-users',
  templateUrl: './changePass.component.html'
})

export class ChangePassComponent {
  email: string;
  user: User;
  repeatedPassword: string;
  password: string;

  constructor(private router: Router, private route: ActivatedRoute, private service: ChangePassService) {
    this.user = new User();
    const user = JSON.parse(localStorage.getItem('user'));
    this.user = user;
  }

  onSubmit() {
    if (this.user.firstLogin) {
      this.updatePassword(this.user.email, this.password);
    } else {
      this.justUpdatePassword(this.user.email, this.password);
    }

  }

  get filled() {
    if (this.password === '' || this.password == null || this.repeatedPassword === '' || this.repeatedPassword == null) {
      return false;
    } else { return true; }
  }
  get passwordsMatch() {
    return this.password === this.repeatedPassword;
  }

  updatePassword(email: string, newpassword: string) {
    this.service.updatePassword(email, newpassword).subscribe(result => { this.router.navigate(['/']);
                                                                          localStorage.clear();
                                                                          alert('You have successfully changed password!' +
        'You can use your account now, please log in');
    }, error => { alert('Error'); });
  }

  private justUpdatePassword(email: string, password: string) {
    this.service.justUpdatePassword(email, password).subscribe(result => { this.router.navigate(['/']);
                                                                           alert('Successfully changed password');
    }, error => { alert('Error'); });
  }
}