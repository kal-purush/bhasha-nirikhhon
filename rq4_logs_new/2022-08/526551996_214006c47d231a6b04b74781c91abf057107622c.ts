import { Injectable, UnauthorizedException } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { catchError, map, Observable } from 'rxjs';
import { OIDCTokenResponse } from '../oidc-token-response.dto';

@Injectable()
export class AdminAuthService {
  accessToken: string;
  refreshToken: string;
  refreshTokenTimeout;

  constructor(private http: HttpService) {}

  /**
   * Login the admin with the given credentials.
   * After successful login, the session is kept alive using refresh tokens.
   * @param username of admin
   * @param password of admin
   * @returns OIDCTokenResponse
   */
  login(username: string, password: string): Observable<OIDCTokenResponse> {
    return this.credentialAuth(username, password);
  }

  private credentialAuth(username: string, password: string) {
    const body = new URLSearchParams();
    body.set('username', username);
    body.set('password', password);
    body.set('grant_type', 'password');
    return this.getToken(body);
  }

  private refreshTokenAuth() {
    const body = new URLSearchParams();
    body.set('refresh_token', this.refreshToken);
    body.set('grant_type', 'refresh_token');
    return this.getToken(body);
  }

  private getToken(body: URLSearchParams): Observable<OIDCTokenResponse> {
    body.set('client_id', 'admin-cli');
    const obs = this.http
      .post<OIDCTokenResponse>(
        'https://keycloak.aam-digital.com/realms/master/protocol/openid-connect/token',
        body.toString(),
      )
      .pipe(
        map((res) => res.data),
        catchError(() => {
          throw new UnauthorizedException();
        }),
      );
    obs.subscribe((res) => {
      this.accessToken = res.access_token;
      this.refreshToken = res.refresh_token;
      this.refreshTokenBeforeExpiry(res.expires_in);
    });
    return obs;
  }

  private refreshTokenBeforeExpiry(secondsTillExpiration: number) {
    // Refresh token one minute before it expires or after ten seconds
    const refreshTimeout = Math.max(50, secondsTillExpiration - 60);
    clearTimeout(this.refreshTokenTimeout);
    this.refreshTokenTimeout = setTimeout(
      () => this.refreshTokenAuth(),
      refreshTimeout * 1000,
    );
  }
}