import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { AuthResponse } from './auth-response.interface';
import { environment } from '../../environments/environment';

@Injectable({
	providedIn: 'root'
})
export class AuthService {

	constructor(private http: HttpClient) { }

	signup(email: string, password: string): Observable<AuthResponse> {
		return this.http.post<AuthResponse>(
			'https://identitytoolkit.googleapis.com/v1/accounts:signUp?key=' + environment.userAPIKey,
			{
				email: email,
				password: password,
				returnSecureToken: true
			}
		);
	}
}