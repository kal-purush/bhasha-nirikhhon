import {Component, OnInit} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {User} from '../models/user.model';

@Component({
  // tslint:disable-next-line:component-selector
  selector: 'app-profile',
  templateUrl: './profile.component.html',
  styleUrls: ['./profile.component.css']
})

export class ProfileComponent implements OnInit {
  private user: User;
  constructor(private router: Router, private route: ActivatedRoute) {
    this.user = new User();
  }
  ngOnInit(): void {
    const user = JSON.parse(localStorage.getItem('user'));
    this.user = user;
  }
}