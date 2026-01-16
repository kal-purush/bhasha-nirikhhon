import { Component, OnInit } from '@angular/core';
import {Filter, FilterService} from "../../filter-bar/filter.service";
import {UserService} from "../user.service";
import {NotificationService} from "../../notification/notification.service";
import {Router} from "@angular/router";

@Component({
  selector: 'app-connect-local',
  templateUrl: './connect-local.component.html',
  styleUrls: ['./connect-local.component.scss']
})
export class ConnectLocalComponent implements OnInit {

  constructor(private _filterService: FilterService,
              private _userService: UserService,
              private _notificationService: NotificationService,
              private _router: Router) { }

  ngOnInit() {
    this._filterService.update(new Filter({not_displayed: true}));
  }

  //noinspection JSUnusedGlobalSymbols
  connect(event, username, password) {
    event.preventDefault();


    this._userService.connectLocal(username, password)
      .then(() => {
        this._router.navigate(['/profile']);
      })
      .catch( (err) => {
        this._notificationService.error(err);
      });
  }

}