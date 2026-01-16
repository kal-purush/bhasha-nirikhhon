import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { User } from '../models/User'

@Injectable()
export class UserService {
    public userId: string;

    constructor(private http: HttpClient) {
        // set token if saved in local storage
        var currentUser = JSON.parse(localStorage.getItem('currentUser'));
        this.userId = currentUser && currentUser.userId;
    }

    public GetCurrentUser(): Observable<User> {
        return this.http.get<User>('api/user/' + this.userId); 
    }
}