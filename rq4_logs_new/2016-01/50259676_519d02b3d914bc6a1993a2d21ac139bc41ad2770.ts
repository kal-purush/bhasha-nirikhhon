import {Component, OnInit} from 'angular2/core';
import {UserMenuItemComponent} from './user-menu-item.component'
import {UserService} from './user.service'

@Component({
    selector: 'educraft-editor',
    template: `
<div class="ui menu">
    <a class="active item">EduCraft</a>

    <user-menu-item [user]="user" class="right menu"></user-menu-item>
</div>
`,
    directives: [UserMenuItemComponent],
    providers: [UserService]
})
export class AppComponent implements OnInit {
    public user = null;

    constructor(private _userService:UserService) {
    }

    ngOnInit() {
        this._userService.getUser().subscribe(user => this.user = user.json());
    }
}