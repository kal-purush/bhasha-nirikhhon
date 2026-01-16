import { Injectable } from '@angular/core';
import { User, AGSServerResponse } from 'src/types';
import { Observable, of } from 'rxjs';

@Injectable({
	providedIn: 'root'
})
export class LoginService {

	constructor() { }

	public login(user: User): Observable<AGSServerResponse> {
		if (user.username && user.password) {
			const id = `ags-${new Date().getTime()}`;
			localStorage.setItem('ags-session', id);
			return of({ success: true });
		}

		return of({ success: false, message: 'Fill all required data' });
	}
}