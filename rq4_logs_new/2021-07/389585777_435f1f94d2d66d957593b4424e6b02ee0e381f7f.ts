import {Component, Input, OnInit} from '@angular/core';
import {IUser} from "../../models/IUser";
import {UserService} from "../../services/user.service";
import {IUserJSON} from "../../models/IUserJSON";

@Component({
  selector: 'app-user',
  templateUrl: './user.component.html',
  styleUrls: ['./user.component.css']
})
export class UserComponent implements OnInit {

  @Input()
  user : IUser;

  @Input()
  userJSON : IUserJSON;




  constructor(private UserService : UserService) {
    this.UserService.doSomeStuff()
  }

  ngOnInit(): void {
  }

}