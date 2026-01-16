import { Component, Injectable } from '@angular/core';
import { Headers, Http } from '@angular/http';
import 'rxjs';

@Injectable()
export class UserService {

    // this is where the variables go

    apiUrl: string;

    constructor(
        private http: Http
    ) {
        // do init stuff
        this.apiUrl = 'http://localhost:8000/api'
    }
    // this is where the function goes..
    getUsers(): Promise<Array<Object>> {
        return this.http.get(`${this.apiUrl}/users`).toPromise().then((resp) => {
            let users = resp.json();
            console.log('users', users);
            return users;
        });
    }

    getUserById(userId): Promise<Object> {
        return this.http.get(`${this.apiUrl}/users/id/${userId}`).toPromise().then((resp) => {
            let user = resp.json();
            console.log('user', user);
            return user;
        });
    }

    addUser(user): Promise<Object> {
        return this.http.post(`${this.apiUrl}/users`, user).toPromise().then((resp) => {
            let event = resp.json();
            console.log('user', user);
            return user;
        });
    }

    deleteUser(id): Promise<Object> {
        console.log(`from user.service delete method......`);
        return this.http.delete(`${this.apiUrl}/users/id/${id}`).toPromise().then((resp) => {
            let status = resp.json();
            console.log('user', status);
            return status;
        });
    }

    updateUser(id, user): Promise<Object> {
        return this.http.put(`${this.apiUrl}/users/id/${id}`, user).toPromise().then((resp) => {
            let event = resp.json();
            console.log('user', user);
            return user;
        });
    }
    

}