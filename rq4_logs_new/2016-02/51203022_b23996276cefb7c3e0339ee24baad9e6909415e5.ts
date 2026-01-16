import {Component} from 'angular2/core';
import {Router} from 'angular2/router';
import {UserService} from '../services/user.service';
import {User} from '../contracts/user';

@Component({
    selector: 'new-user',
    templateUrl: 'app/templates/new-user.html',
    providers: [UserService]
})
export class NewUserComponent {

    constructor (private _userService: UserService, private _router: Router) { }

    public addUser(firstName: string, lastName: string): void {
        let user: User = new User(firstName, lastName);
        this._userService.addUser(user);

        this._router.navigate(['Users']);
    }
}