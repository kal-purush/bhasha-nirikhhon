import { HttpClient } from '@angular/common/http';
import { inject, Injectable, signal } from '@angular/core';
import { environment } from '../../../environments/environment';
import { User } from '../../shared/interfaces/user';
import { Router } from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private httpClient = inject(HttpClient);
  private readonly baseUrl = `${environment.apiUrl}/auth`;
  readonly user = signal<User | null>(null);
  private router = inject(Router);

  logInWithLinkedIn() {
    const url = `${this.baseUrl}/loginWithLinkedIn`;
    window.location.href = url;
  }

  startUserSession(token: string) {
    const url = `${this.baseUrl}/startUserSession`;
    this.httpClient.get(url, { params: { access_token: token }, withCredentials: true }).subscribe({
      next: () => {
        this.getUser();
        this.router.navigate(['/']);
      },
      error: (error) => {
        console.error('Error setting access token as cookie:', error);
      }
    });
  }

  getUser() {
    const url = `${this.baseUrl}/user`;
    this.httpClient.get<User>(url, { withCredentials: true }).subscribe({
      next: (response) => {
        this.user.set(response);
      },
      error: (error) => {
        console.error('Error fetching user data:', error);
      }
    });
  }
}