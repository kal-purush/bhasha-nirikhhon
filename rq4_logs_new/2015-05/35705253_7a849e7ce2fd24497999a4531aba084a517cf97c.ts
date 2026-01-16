/// <reference path="../typings/tsd" />

import { Component, View, bootstrap, For } from 'angular2/angular2';
import { UserDataService, IUser } from './services/user-data-service';
import { EditUser } from './components/edit-user'

@Component({
  selector: 'app',
  injectables: [UserDataService]
})
@View({
  template: `
    <div class="row">
        <div class="col-xs-12 col-sm-6 col-md-3">
            <div class="panel panel-default">
                <!-- Default panel contents -->
                <div class="panel-heading">Users</div>
                <div class="panel-body">
                    <button class="btn btn-primary">Create User</button>
                </div>
                <!-- List group -->
                <div class="list-group">
                    <a class="list-group-item" *for="#user of users" [class.active]="(editUser==user)"
                       (click)="selectUser(user)">
                        {{ user.name }} ({{ user.email }})
                    </a>
                </div>
            </div>
        </div>
        <div class="col-xs-12 col-sm-6 col-md-9">
            <edit-user [user]="editUser"></edit-user>
        </div>
    </div>
  `,
  directives: [For, EditUser]
})
class AppComponent {
  users: Array<IUser>;
  editUser: string;


  constructor(protected userDataService: UserDataService) {
    this.users = this.userDataService.getUsers();
  }

  public selectUser(user) {
    this.editUser = user;
  }
}

bootstrap(AppComponent);