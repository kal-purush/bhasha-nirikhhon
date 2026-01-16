import {Component, OnInit} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {UserService} from '../../service/user.service';
import {Location} from '@angular/common';
import {User} from '../../entity/user';

@Component({
  selector: 'navigation-panel',
  templateUrl: './navigation-panel.component.html',
  styleUrls: ['./navigation-panel.component.css']
})
export class NavigationPanelComponent implements OnInit {

  constructor(private userService: UserService,
              private router: Router,
              private location: Location,
              private route: ActivatedRoute) {
  }

  ngOnInit() {
  }
}