import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';

import { Store } from '@ngrx/store';
import * as usersActions from '../actions/users';

import { AppStore } from '../models/app-store';

import { User } from '../models/user';

@Injectable()
export class UsersService {
    users: Observable<Array<User>>;
    usersArray: User[] = [];

    private API_PATH: string = 'https://jsonplaceholder.typicode.com/users';

    constructor(private http: Http, private store: Store<AppStore>) {
        this.users = store.select('users');
    }

    fetchUsers() {
        this.http.get(this.API_PATH)
            .map(res => res.json())
            .map(payload => ({ type: usersActions.ActionTypes.FETCH, payload }))
            .subscribe(action => { 
                this.store.dispatch(action); 
                this.usersArray = action.payload;
            });
    }

    getAuthor(userId: number): User {
        return this.usersArray.find(user => user.id === userId );
    }
}