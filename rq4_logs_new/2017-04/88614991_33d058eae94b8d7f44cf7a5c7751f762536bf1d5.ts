import { Injectable } from '@angular/core';
import { HttpService } from './http.service';
import { BehaviorSubject } from 'rxjs';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import { IUser } from '../models';

@Injectable()
export class AuthService {

    user$: BehaviorSubject<IUser> = new BehaviorSubject(this.emptyUser());
    constructor(private _hs: HttpService) {

    }

    login(obj: Object) {
        this._hs.PostRequest('/api/auth', obj)
            .subscribe(res => {
                if (res.success) {
                    console.log('res', res);
                    let user = res['data'];
                    localStorage.setItem('user', JSON.stringify(user));
                    this.user$.next(user);
                }
                else {
                    console.log('error', res);
                }
            },
            err => {
                console.error('err', err.json());
            })
    }
    emptyUser() {
		return {
			id: '',
			username: '',
            email: '',
            role: '',
            token: ''
		}
	}
}