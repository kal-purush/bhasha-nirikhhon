import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';

@Injectable({
  providedIn: 'root',
})
export class UserProfileService {
  private apiUrl = 'https://buddy-board-infosys-internship-oct2024.onrender.com/api/user'; // Replace with the actual user API endpoint

  constructor(private http: HttpClient) {}

  /**
   * Retrieves the auth token from localStorage.
   * @returns The stored token or null if not found.
   */
  private getAuthToken(): string | null {
    if (typeof window !== 'undefined' && localStorage) {
      return localStorage.getItem('token');
    }
    return null;
  }

  /**
   * Helper method to construct authorization headers.
   * @returns An instance of HttpHeaders with the Authorization header if a token is present.
   */
  private createAuthHeaders(): HttpHeaders {
    const token = this.getAuthToken();
    return new HttpHeaders({
      'Content-Type': 'application/json',
      Authorization: token ? `Bearer ${token}` : '',
    });
  }

  /**
   * Fetches user profile data from the server based on the username.
   * @param username The username of the profile to fetch.
   * @returns Observable containing user data or an error if the request fails.
   */
  public getUserProfile(username: string): Observable<any> {
    const headers = this.createAuthHeaders();
    return this.http.get(`${this.apiUrl}/${username}`, { headers }).pipe(
      catchError((error) => {
        console.error('Error fetching user profile:', error);
        return throwError(() => new Error('Failed to fetch user profile data. Please try again.'));
      })
    );
  }
}